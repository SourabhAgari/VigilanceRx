package com.healthcare.rxvigilance.domain;

/**
 * Represents a drug class and whether it is eligible for tracking.
 *
 * @param drugClass the drug class identifier
 * @param trackable whether the drug class should be tracked
 */
public record DrugClassRef(
        String drugClass,
        boolean trackable
) {
}
