/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client.opensearch._helpers.bulk;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Adaptive controller that automatically tunes {@link BulkIngester#maxOperations} at runtime
 * by observing server-side indexing latency ({@code BulkResponse.took()}) after every bulk response.
 *
 * <h2>Algorithm: Hill Climbing + AIMD Decrease</h2>
 * <ul>
 *   <li><b>Hill climbing</b> — increases {@code maxOperations} additively while throughput
 *       ({@code batchSize / took}) keeps improving. Stops at the plateau where further increases
 *       yield less than {@link #minImprovementFraction} gain. Step size halves on each regression
 *       to enable fine-grained search near the optimum.</li>
 *   <li><b>AIMD multiplicative decrease</b> — when 429s or transport failures are detected,
 *       {@code maxOperations} is immediately reduced by {@link #overloadDecreaseFactor} and the
 *       hill climb resets from the new baseline.</li>
 * </ul>
 *
 * <h2>Throughput metric</h2>
 * {@code measuredThroughput = batchSize / tookMs * 1000} (operations per second, per batch).
 * An EWMA with {@link #ewmaAlpha} smooths out transient spikes before comparing against
 * {@link #bestThroughput}.
 *
 * <h2>Thread safety</h2>
 * {@link #observe} is always called inside {@link BulkIngester}'s {@code ReentrantLock}
 * (via {@code sendRequestCondition.signalIfReadyAfter}), so no additional synchronization
 * is needed inside this class.
 *
 * @see BulkIngester
 */
public class AdaptiveOperationsController {

    private static final Log logger = LogFactory.getLog(AdaptiveOperationsController.class);

    /**
     * Current lifecycle state of the controller.
     */
    public enum State {
        /** Actively increasing {@code maxOperations} toward the throughput optimum. */
        CLIMBING,
        /** Throughput plateau detected — holding {@code maxOperations} at {@link #bestOps}. */
        HOLD,
        /**
         * Step size decayed below {@link #minStep} after repeated regressions —
         * {@code maxOperations} is pinned at the discovered optimum until an overload event
         * resets the search.
         */
        CONVERGED
    }

    // ---- configuration (immutable after construction) ----

    private final int minOps;
    private final int maxOps;
    private final int initialStep;
    private final int minStep;
    private final double minImprovementFraction;
    private final double dropThreshold;
    private final double overloadDecreaseFactor;
    private final double ewmaAlpha;

    // ---- mutable state (always mutated under BulkIngester's ReentrantLock) ----

    private volatile int currentOps;
    private int bestOps;
    private double bestThroughput;
    private double ewmaThroughput;
    private int currentStep;
    private State state;

    // ---- observability counters ----

    private long totalIncreases;
    private long totalDecreases;

    /**
     * Creates a new controller with default tuning parameters.
     *
     * @param initialOps starting value for {@code maxOperations}
     */
    public AdaptiveOperationsController(int initialOps) {
        this(
            initialOps,
            10,          // minOps
            10_000,      // maxOps
            Math.max(10, initialOps),  // initialStep ≈ initialOps for exponential-like ramp
            25,          // minStep
            0.005,       // minImprovementFraction (0.5%)
            0.10,        // dropThreshold (10% throughput drop = regression)
            0.75,        // overloadDecreaseFactor
            0.3          // ewmaAlpha
        );
    }

    /**
     * Creates a new controller with explicit tuning parameters.
     *
     * @param initialOps              starting value for {@code maxOperations}
     * @param minOps                  hard floor — {@code maxOperations} never goes below this
     * @param maxOps                  hard ceiling — {@code maxOperations} never goes above this
     * @param initialStep             additive step size at the start of the hill climb
     * @param minStep                 step size below which the controller transitions to
     *                                {@link State#CONVERGED}
     * @param minImprovementFraction  minimum fractional throughput improvement required to
     *                                continue increasing (e.g. {@code 0.005} = 0.5%)
     * @param dropThreshold           fractional throughput drop that triggers step-halving
     *                                (e.g. {@code 0.10} = 10%)
     * @param overloadDecreaseFactor  multiplicative factor applied on 429 / transport failure
     *                                (e.g. {@code 0.75} = reduce by 25%)
     * @param ewmaAlpha               EWMA smoothing coefficient for throughput ({@code 0 < α ≤ 1})
     */
    public AdaptiveOperationsController(
        int initialOps,
        int minOps,
        int maxOps,
        int initialStep,
        int minStep,
        double minImprovementFraction,
        double dropThreshold,
        double overloadDecreaseFactor,
        double ewmaAlpha
    ) {
        this.currentOps = Math.max(minOps, Math.min(maxOps, initialOps));
        this.bestOps = this.currentOps;
        this.bestThroughput = 0;
        this.ewmaThroughput = 0;
        this.currentStep = Math.max(minStep, initialStep);
        this.state = State.CLIMBING;

        this.minOps = minOps;
        this.maxOps = maxOps;
        this.initialStep = initialStep;
        this.minStep = minStep;
        this.minImprovementFraction = minImprovementFraction;
        this.dropThreshold = dropThreshold;
        this.overloadDecreaseFactor = overloadDecreaseFactor;
        this.ewmaAlpha = ewmaAlpha;
    }

    // -----------------------------------------------------------------------
    // Core observation method — called once per bulk response
    // -----------------------------------------------------------------------

    /**
     * Observes the result of a completed bulk request and returns the new {@code maxOperations}
     * value that the ingester should apply for the next batch.
     *
     * <p>Must be called under the ingester's {@code ReentrantLock}.
     *
     * @param batchSize  number of operations in the completed bulk request
     * @param tookMs     server-side indexing time reported by {@code BulkResponse.took()}; pass
     *                   {@code -1} if the response was not available (transport failure)
     * @param has429     {@code true} if any item in the response had HTTP status 429
     * @param failure    {@code true} if the request failed at the transport level
     * @return the new {@code maxOperations} value to apply
     */
    public int observe(int batchSize, long tookMs, boolean has429, boolean failure) {

        // --- Hard overload signals: AIMD multiplicative decrease ---
        if (failure || has429) {
            return applyOverload("overload: " + (failure ? "transport failure" : "429 response"));
        }

        // tookMs == -1 means no response (failure already handled above; guard for safety)
        if (tookMs <= 0) {
            return currentOps;
        }

        // --- Update EWMA throughput (ops/s) ---
        double rawThroughput = (double) batchSize / tookMs * 1000.0;
        ewmaThroughput = ewmaAlpha * rawThroughput + (1 - ewmaAlpha) * ewmaThroughput;

        // --- Hill climbing ---
        if (state == State.CONVERGED) {
            // Stay converged until an overload event resets us
            return currentOps;
        }

        if (ewmaThroughput > bestThroughput * (1 + minImprovementFraction)) {
            // Still improving — update best, increase ops
            bestThroughput = ewmaThroughput;
            bestOps = currentOps;
            int next = Math.min(currentOps + currentStep, maxOps);
            if (next != currentOps) {
                logger.debug("adaptive ops: improving tput=" + ewmaThroughput + " → increase " + currentOps + " → " + next);
                currentOps = next;
                totalIncreases++;
                state = State.CLIMBING;
            } else {
                // Already at maxOps ceiling
                state = State.HOLD;
            }

        } else if (ewmaThroughput < bestThroughput * (1 - dropThreshold)) {
            // Regression — step size halves, revert to bestOps
            totalDecreases++;
            currentStep = Math.max(minStep, currentStep / 2);
            logger.debug("adaptive ops: regression tput=" + ewmaThroughput
                + " bestTput=" + bestThroughput + " → revert to bestOps=" + bestOps + ", step=" + currentStep);
            if (currentStep <= minStep) {
                // Search resolution exhausted — pin at best
                currentOps = bestOps;
                state = State.CONVERGED;
                logger.info("adaptive ops: converged at maxOperations=" + bestOps);
            } else {
                // Try the next smaller step from bestOps
                currentOps = Math.min(bestOps + currentStep, maxOps);
                state = State.CLIMBING;
            }

        } else {
            // Plateau — throughput not improving enough, not dropping either
            state = State.HOLD;
            logger.debug("adaptive ops: plateau tput=" + ewmaThroughput + " → hold at ops=" + currentOps);
        }

        return currentOps;
    }

    // -----------------------------------------------------------------------
    // Overload path
    // -----------------------------------------------------------------------

    private int applyOverload(String reason) {
        int next = Math.max(minOps, (int) (currentOps * overloadDecreaseFactor));
        logger.warn("adaptive ops: " + reason + " → decrease " + currentOps + " → " + next);
        currentOps = next;
        bestOps = next;
        bestThroughput = 0;           // reset hill climb from new baseline
        ewmaThroughput = 0;
        currentStep = Math.max(minStep, initialStep);
        state = State.CLIMBING;
        totalDecreases++;
        return currentOps;
    }

    // -----------------------------------------------------------------------
    // Observability getters
    // -----------------------------------------------------------------------

    /**
     * Returns the current effective {@code maxOperations} value.
     */
    public int currentMaxOps() {
        return currentOps;
    }

    /**
     * Returns the {@code maxOperations} value at which the highest throughput was observed.
     */
    public int bestOps() {
        return bestOps;
    }

    /**
     * Returns the EWMA-smoothed throughput in operations per second.
     */
    public double ewmaThroughput() {
        return ewmaThroughput;
    }

    /**
     * Returns the current lifecycle state of the controller.
     */
    public State state() {
        return state;
    }

    /**
     * Returns the total number of times {@code maxOperations} was increased.
     */
    public long totalIncreases() {
        return totalIncreases;
    }

    /**
     * Returns the total number of times {@code maxOperations} was decreased (regressions + overloads).
     */
    public long totalDecreases() {
        return totalDecreases;
    }

    @Override
    public String toString() {
        return "AdaptiveOperationsController{"
            + "state=" + state
            + ", currentOps=" + currentOps
            + ", bestOps=" + bestOps
            + ", ewmaTput=" + String.format("%.3f", ewmaThroughput)
            + ", step=" + currentStep
            + ", increases=" + totalIncreases
            + ", decreases=" + totalDecreases
            + '}';
    }
}
