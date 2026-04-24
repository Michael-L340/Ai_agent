# Scoring System

This project now uses an explainable multi-dimensional scoring pipeline.

It also supports a lightweight feedback-driven policy layer:

- human feedback can update scoring weights
- boost / penalty rules are persisted and versioned
- the active policy is read before each scoring run
- every score stores its rule context for auditing

## Input

Scoring consumes:

- structured DD profiles from `dd_reports`
- source count and DD completeness
- planner search plan context
- entity verification metadata

## Component scores

Each component score is normalized to a 0-5 range:

- `business_score`
- `team_score`
- `funding_score`
- `traction_score`
- `market_score`
- `thesis_fit_score`
- `evidence_score`

## Formula

1. Compute the weighted raw score from the seven component scores.
2. Convert the weighted result into a 0-100 `raw_score`.
3. Apply a `confidence_multiplier` derived from `source_hits`, DD completeness, and evidence confidence.
4. Subtract `penalty_score` for noisy, unstable, or conflict-prone entities.
5. Add `boost_score` from matched policy rules.
6. Produce `final_score`.

In short:

`final_score = raw_score * confidence_multiplier - penalty_score + boost_score`

## Thesis fit

`thesis_fit_score` is no longer a single opaque match score. It is decomposed into:

- `long_memory_match`
- `short_theme_match`
- `keyword_match`
- `commercial_signal_match`
- `human_preference_match`

The final `thesis_fit_score` is the average of those five sub-scores, and the breakdown is stored with the score result for auditing.

Inputs come from:
- planner `search_plan`
- planner long memory
- planner short memory
- planner human preferences
- structured DD business / traction / market evidence

## Feedback policy

The current policy snapshot stores:

- weights for the seven component scores
- boost rules learned from positive feedback
- penalty rules learned from negative feedback
- policy version and source feedback metadata

Supported feedback types:

- `like`
- `dislike`
- `skip`
- `wrong_entity`
- `prefer_sector`

The policy update is deliberately lightweight:

- `like` and `prefer_sector` tend to boost thesis / market / traction signals
- `dislike` and `skip` tend to increase penalties or downweight weak evidence
- `wrong_entity` adds stronger blocking penalties for the mismatched subject

Every scoring result keeps the policy version and matched policy rules in its explanation so the outcome can be reviewed later.

## Hard gates

Formal recommendations must pass these hard gates:

- `entity_type == company`
- `verification_status == verified`
- `source_hits >= 2`
- `dd_status in ('dd_partial', 'dd_done')`

If a lead scores well numerically but fails a hard gate, it is capped below formal recommendation and will not be pushed.

## Recommendation bands

- Strong Recommend: >= 90
- Recommend: 82-89
- Watchlist: 75-81
- Track Only: 60-74
- Reject: < 60

## Auditing

Every scoring run stores:

- all sub-scores
- the raw score
- the confidence multiplier
- the boost score
- the penalty score
- the final score
- the recommendation band
- the recommendation reason
- the thesis-fit breakdown
- the matched policy rules
- the policy version

This makes each score reproducible and reviewable later.

