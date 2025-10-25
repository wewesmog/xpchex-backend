from datetime import date
from typing import List, Dict, Any
from app.models.daily_summary_models import (
    RoadmapAnalysis, ExecutionPlan, StrategicInitiative,
    Quarter, SuccessMetric, KeyDeliverable, ResourceAllocation
)
from app.shared_services.logger_setup import setup_logger
from app.shared_services.db import get_postgres_connection

logger = setup_logger()

def get_reviews_with_roadmap(date: date, app_id: str) -> List[dict]:
    conn = get_postgres_connection("daily_reviews_view")
    try:
        with conn.cursor() as cur:
            query = """
            SELECT content, latest_analysis ->> 'roadmap' as roadmap FROM processed_app_reviews
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

def aggregate_success_metrics(metrics_list: List[Dict]) -> List[SuccessMetric]:
    """Aggregate success metrics across reviews."""
    metrics_map = {}
    
    for metrics in metrics_list:
        for metric in metrics:
            key = (metric['metric'], metric['data_source'])
            if key not in metrics_map:
                metrics_map[key] = metric
            else:
                # Update current values if they differ
                existing = metrics_map[key]
                if existing['current_value'] != metric['current_value']:
                    existing['current_value'] = max(existing['current_value'], metric['current_value'])

    return [SuccessMetric(**metric) for metric in metrics_map.values()]

def aggregate_quarters(quarters_list: List[Dict]) -> List[Quarter]:
    """Aggregate quarterly plans across reviews."""
    quarters_map = {}
    
    for quarters in quarters_list:
        for quarter in quarters:
            if quarter['quarter'] not in quarters_map:
                quarters_map[quarter['quarter']] = quarter
            else:
                existing = quarters_map[quarter['quarter']]
                # Merge deliverables
                existing_deliverables = {d['initiative_id']: d for d in existing['key_deliverables']}
                for deliverable in quarter['key_deliverables']:
                    if deliverable['initiative_id'] not in existing_deliverables:
                        existing['key_deliverables'].append(deliverable)
                
                # Merge resource allocations
                existing_resources = {r['team']: r for r in existing['resource_allocation']}
                for resource in quarter['resource_allocation']:
                    if resource['team'] not in existing_resources:
                        existing['resource_allocation'].append(resource)
                    else:
                        # Average the allocation percentages
                        existing_res = existing_resources[resource['team']]
                        existing_res['allocation_percentage'] = (
                            existing_res['allocation_percentage'] + resource['allocation_percentage']
                        ) // 2

    return [Quarter(**quarter) for quarter in quarters_map.values()]

def aggregate_strategic_initiatives(initiatives_list: List[Dict]) -> List[StrategicInitiative]:
    """Aggregate strategic initiatives across reviews."""
    initiatives_map = {}
    
    for initiatives in initiatives_list:
        for initiative in initiatives:
            if initiative['id'] not in initiatives_map:
                initiatives_map[initiative['id']] = initiative
            else:
                existing = initiatives_map[initiative['id']]
                # Update confidence scores and impact assessments if they differ
                if existing['prioritization']['overall_score'] < initiative['prioritization']['overall_score']:
                    existing['prioritization'] = initiative['prioritization']
                if existing['impact_assessment']['business_goals'][0]['impact_score'] < \
                   initiative['impact_assessment']['business_goals'][0]['impact_score']:
                    existing['impact_assessment'] = initiative['impact_assessment']

    return [StrategicInitiative(**initiative) for initiative in initiatives_map.values()]

def summarize_roadmap_for_date(date: date, app_id: str) -> RoadmapAnalysis:
    """Summarize roadmap analysis for a specific date and app."""
    reviews = get_reviews_with_roadmap(date, app_id)
    logger.info(f"Processing roadmap analysis for {len(reviews)} reviews")

    if not reviews:
        return RoadmapAnalysis(
            analysis_error="No reviews found for the specified date",
            execution_plan=ExecutionPlan(quarters=[], success_metrics=[]),
            strategic_initiatives=[]
        )

    all_quarters = []
    all_metrics = []
    all_initiatives = []

    for review in reviews:
        roadmap_data = review.get('roadmap')
        if not roadmap_data:
            continue

        # Parse JSON if needed
        if isinstance(roadmap_data, str):
            try:
                import json
                roadmap_data = json.loads(roadmap_data)
            except json.JSONDecodeError:
                logger.error("Failed to parse roadmap JSON")
                continue

        # Extract components
        execution_plan = roadmap_data.get('execution_plan', {})
        quarters = execution_plan.get('quarters', [])
        metrics = execution_plan.get('success_metrics', [])
        initiatives = roadmap_data.get('strategic_initiatives', [])

        all_quarters.append(quarters)
        all_metrics.append(metrics)
        all_initiatives.append(initiatives)

    # Aggregate components
    aggregated_quarters = aggregate_quarters(all_quarters)
    aggregated_metrics = aggregate_success_metrics(all_metrics)
    aggregated_initiatives = aggregate_strategic_initiatives(all_initiatives)

    # Create execution plan
    execution_plan = ExecutionPlan(
        quarters=aggregated_quarters,
        success_metrics=aggregated_metrics
    )

    # Return final analysis
    return RoadmapAnalysis(
        execution_plan=execution_plan,
        strategic_initiatives=aggregated_initiatives
    )

if __name__ == "__main__":
    test_date = date(2025, 3, 11)
    test_app_id = "com.kcb.mobilebanking.android.mbp"
    
    roadmap = summarize_roadmap_for_date(test_date, test_app_id)
    if roadmap:
        logger.info("Roadmap Analysis Summary:")
        logger.info(f"Quarters planned: {len(roadmap.execution_plan.quarters)}")
        logger.info(f"Success metrics defined: {len(roadmap.execution_plan.success_metrics)}")
        logger.info(f"Strategic initiatives: {len(roadmap.strategic_initiatives)}")
        
        for quarter in roadmap.execution_plan.quarters:
            logger.info(f"\nQuarter {quarter.quarter} Theme: {quarter.theme}")
            logger.info(f"Key deliverables: {len(quarter.key_deliverables)}")
            for deliverable in quarter.key_deliverables:
                logger.info(f"  - {deliverable.deliverable}")
    else:
        logger.warning("No roadmap analysis available for the specified date") 