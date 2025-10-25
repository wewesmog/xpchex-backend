from datetime import date
from typing import List, Dict, Any
from app.models.daily_summary_models import (
    ActionItemsAnalysis, Issue, Action, BusinessImpact,
    IssueContext
)
from app.shared_services.logger_setup import setup_logger
from app.shared_services.db import get_postgres_connection

logger = setup_logger()

def get_reviews_with_action_items(date: date, app_id: str) -> List[dict]:
    conn = get_postgres_connection("daily_reviews_view")
    try:
        with conn.cursor() as cur:
            query = """
            SELECT content, latest_analysis ->> 'action_items' as action_items FROM processed_app_reviews
            WHERE date(review_created_at) = %s::date AND app_id = %s 
            """
            logger.info(f"Executing query: {str(cur.mogrify(query, (date, app_id)))[:500]}...")
            cur.execute(query, (date, app_id))

            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

            reviews = [dict(zip(columns, row)) for row in rows]
            logger.info(f"Fetched {len(rows)} reviews for date {date} and app_id {app_id}")
            return reviews
    except Exception as e:
        logger.error(f"Error fetching reviews: {str(e)[:500]}...")
        raise e
    finally:
        conn.close()

def aggregate_issues(issues_list: List[Dict]) -> List[Issue]:
    """Aggregate issues across reviews."""
    issues_map = {}
    
    for issues in issues_list:
        for issue in issues:
            issue_id = issue.get('id')
            if issue_id not in issues_map:
                issues_map[issue_id] = issue
            else:
                existing = issues_map[issue_id]
                # Update confidence and priority if higher
                if issue.get('confidence', 0) > existing.get('confidence', 0):
                    existing['confidence'] = issue['confidence']
                if issue.get('priority_score', 0) > existing.get('priority_score', 0):
                    existing['priority_score'] = issue['priority_score']
                # Update investigation status
                existing['needs_investigation'] = existing['needs_investigation'] or issue.get('needs_investigation', False)

    return [Issue(**issue) for issue in issues_map.values()]

def aggregate_actions(actions_list: List[Dict]) -> List[Action]:
    """Aggregate actions across reviews."""
    actions_map = {}
    
    for actions in actions_list:
        for action in actions:
            action_key = (action.get('type'), action.get('issue_id'))
            if action_key not in actions_map:
                actions_map[action_key] = action
            else:
                existing = actions_map[action_key]
                # Update confidence if higher
                if action.get('confidence', 0) > existing.get('confidence', 0):
                    existing['confidence'] = action['confidence']
                # Merge prerequisites
                existing_prereqs = set(existing.get('prerequisites', []))
                new_prereqs = set(action.get('prerequisites', []))
                existing['prerequisites'] = list(existing_prereqs | new_prereqs)

    return [Action(**action) for action in actions_map.values()]

def aggregate_business_impact(impacts: List[Dict]) -> BusinessImpact:
    """Aggregate business impact across reviews."""
    total_severity_score = 0
    total_confidence = 0
    affected_areas = set()
    metrics_impact = {'app_rating': False, 'user_retention': False, 'user_acquisition': False}
    recommendations = []
    
    for impact in impacts:
        total_severity_score += {'low': 1, 'medium': 2, 'high': 3}.get(impact.get('severity', 'low'), 1)
        total_confidence += impact.get('confidence', 0)
        affected_areas.update(impact.get('affected_areas', []))
        
        # Update metrics impact
        impact_metrics = impact.get('metrics_impact', {})
        for metric in metrics_impact:
            metrics_impact[metric] = metrics_impact[metric] or impact_metrics.get(metric, False)
            
        if impact.get('recommendation'):
            recommendations.append(impact['recommendation'])
    
    # Calculate averages
    count = len(impacts) or 1
    avg_severity_score = total_severity_score / count
    severity = 'high' if avg_severity_score > 2 else 'medium' if avg_severity_score > 1 else 'low'
    
    return BusinessImpact(
        severity=severity,
        confidence=total_confidence / count,
        affected_areas=list(affected_areas),
        metrics_impact=metrics_impact,
        recommendation=". ".join(recommendations) if recommendations else "No specific recommendations available."
    )

def summarize_action_items_for_date(date: date, app_id: str) -> ActionItemsAnalysis:
    """Summarize action items analysis for a specific date and app."""
    reviews = get_reviews_with_action_items(date, app_id)
    logger.info(f"Processing action items analysis for {len(reviews)} reviews")

    if not reviews:
        return ActionItemsAnalysis(
            analysis_error="No reviews found for the specified date",
            issues=[],
            actions=[],
            business_impact=BusinessImpact(
                severity="low",
                confidence=0,
                affected_areas=[],
                metrics_impact={'app_rating': False, 'user_retention': False, 'user_acquisition': False},
                recommendation="No data available for analysis"
            ),
            unactionable_reason="No reviews found for analysis"
        )

    all_issues = []
    all_actions = []
    all_impacts = []
    unactionable_reasons = set()

    for review in reviews:
        action_items_data = review.get('action_items')
        if not action_items_data:
            continue

        # Parse JSON if needed
        if isinstance(action_items_data, str):
            try:
                import json
                action_items_data = json.loads(action_items_data)
            except json.JSONDecodeError:
                logger.error("Failed to parse action items JSON")
                continue

        # Extract components
        issues = action_items_data.get('issues', [])
        actions = action_items_data.get('actions', [])
        impact = action_items_data.get('business_impact')
        unactionable = action_items_data.get('unactionable_reason')

        all_issues.append(issues)
        all_actions.append(actions)
        if impact:
            all_impacts.append(impact)
        if unactionable:
            unactionable_reasons.add(unactionable)

    # Aggregate components
    aggregated_issues = aggregate_issues(all_issues)
    aggregated_actions = aggregate_actions(all_actions)
    business_impact = aggregate_business_impact(all_impacts)

    # Return final analysis
    return ActionItemsAnalysis(
        issues=aggregated_issues,
        actions=aggregated_actions,
        business_impact=business_impact,
        unactionable_reason=". ".join(unactionable_reasons) if unactionable_reasons else None
    )

if __name__ == "__main__":
    test_date = date(2025, 3, 11)
    test_app_id = "com.kcb.mobilebanking.android.mbp"
    
    action_items = summarize_action_items_for_date(test_date, test_app_id)
    if action_items:
        logger.info("Action Items Analysis Summary:")
        logger.info(f"Issues found: {len(action_items.issues)}")
        for issue in action_items.issues:
            logger.info(f"\nIssue {issue.id}:")
            logger.info(f"Type: {issue.type}")
            logger.info(f"Severity: {issue.severity}")
            logger.info(f"Description: {issue.description}")
            logger.info(f"Priority Score: {issue.priority_score:.2f}")
        
        logger.info(f"\nActions recommended: {len(action_items.actions)}")
        for action in action_items.actions:
            logger.info(f"\nAction for issue {action.issue_id}:")
            logger.info(f"Type: {action.type}")
            logger.info(f"Description: {action.description}")
            logger.info(f"Timeline: {action.suggested_timeline}")
        
        logger.info("\nBusiness Impact:")
        logger.info(f"Severity: {action_items.business_impact.severity}")
        logger.info(f"Affected Areas: {', '.join(action_items.business_impact.affected_areas)}")
        logger.info(f"Recommendation: {action_items.business_impact.recommendation}")
    else:
        logger.warning("No action items analysis available for the specified date") 