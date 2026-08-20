package com.healthcare.rxvigilance.domain;

/**
 * Represents a drug class reference update for an NDC code.
 *
 * @param ndcCode the National Drug Code identifying the drug
 * @param drugClassRef the drug class reference associated with the NDC
 */
public record DrugClassRefUpdate(
        String ndcCode,
        DrugClassRef drugClassRef
) {
}