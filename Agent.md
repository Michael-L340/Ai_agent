# Project Rules

## Project Goal
This project is an MVP agent system for sourcing investable AI agent security startups.

## Architecture
Main flow:
Interaction -> Planner -> Search -> DD -> Scoring -> Recommendation -> Feedback

## Coding Rules
- Keep implementation simple and explicit
- Prefer small diffs over broad refactors
- Preserve existing API routes unless task explicitly requires changes
- Add or update tests for any changed scoring or planner logic
- Do not introduce new infrastructure like Kafka, Celery, or vector DB unless task explicitly asks for it

## Important Business Constraints
- Recommendation requires score threshold + source evidence threshold
- Planner owns long memory, short memory, and source policy
- Interaction agent is the only human-facing entry
- DD must move toward structured multi-dimensional fields
- Scoring must stay explainable

## Before finishing a task
- Run relevant tests
- Report changed files
- Report assumptions
- Report anything still missing