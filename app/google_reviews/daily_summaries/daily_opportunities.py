from datetime import date
from typing import List, Dict, Any
from app.models.daily_summary_models import (
    OpportunitiesAnalysis, MarketOpportunity, Evidence,
    Implementation, Risk, ImpactAnalysis, CompetitivePosition,
    ReviewSnippet, ImpactMetrics
)
from app.shared_services.logger_setup import setup_logger
from app.shared_services.db import get_postgres_connection

logger = setup_logger()

def get_reviews_with_opportunities(date: date, app_id: str) -> List[dict]:
    conn = get_postgres_connection("daily_reviews_view")
    try:
        with conn.cursor() as cur:
            query = """
            SELECT content, latest_analysis ->> 'opportunities' as opportunities FROM processed_app_reviews
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

def aggregate_evidence(evidences: List[Dict]) -> Evidence:
    """Aggregate evidence across reviews."""
    total_review_count = 0
    total_sentiment = 0
    all_snippets = {}
    total_frequency = 0
    
    for evidence in evidences:
        total_review_count += evidence.get('review_count', 0)
        total_sentiment += evidence.get('user_sentiment', 0) * evidence.get('review_count', 1)
        total_frequency += evidence.get('mention_frequency', 0)
        
        # Aggregate snippets
        for snippet in evidence.get('review_snippets', []):
            text = snippet.get('text', '')
            if text not in all_snippets or snippet.get('sentiment', 0) > all_snippets[text].get('sentiment', 0):
                all_snippets[text] = snippet

    count = len(evidences) or 1
    avg_sentiment = total_sentiment / total_review_count if total_review_count > 0 else 0
    
    return Evidence(
        review_count=total_review_count,
        user_sentiment=avg_sentiment,
        review_snippets=[ReviewSnippet(**snippet) for snippet in all_snippets.values()],
        mention_frequency=total_frequency
    )

def aggregate_implementation(implementations: List[Dict]) -> Implementation:
    """Aggregate implementation details across reviews."""
    all_risks = {}
    all_dependencies = set()
    all_resources = set()
    complexity_scores = {'low': 1, 'medium': 2, 'high': 3}
    total_complexity = 0
    timelines = []
    
    for impl in implementations:
        # Aggregate risks
        for risk in impl.get('risks', []):
            risk_key = risk.get('description', '')
            if risk_key not in all_risks or complexity_scores.get(risk.get('severity', 'low'), 1) > \
               complexity_scores.get(all_risks[risk_key].get('severity', 'low'), 1):
                all_risks[risk_key] = risk
        
        # Aggregate other fields
        all_dependencies.update(impl.get('dependencies', []))
        all_resources.update(impl.get('required_resources', []))
        total_complexity += complexity_scores.get(impl.get('complexity', 'low'), 1)
        if impl.get('estimated_timeline'):
            timelines.append(impl['estimated_timeline'])
    
    # Calculate average complexity
    avg_complexity = total_complexity / (len(implementations) or 1)
    complexity = 'high' if avg_complexity > 2 else 'medium' if avg_complexity > 1 else 'low'
    
    # Choose most common timeline
    timeline = max(timelines, key=timelines.count) if timelines else "undefined"
    
    return Implementation(
        risks=[Risk(**risk) for risk in all_risks.values()],
        complexity=complexity,
        dependencies=list(all_dependencies),
        estimated_timeline=timeline,
        required_resources=list(all_resources)
    )

def aggregate_impact_analysis(analyses: List[Dict]) -> ImpactAnalysis:
    """Aggregate impact analysis across reviews."""
    all_segments = set()
    all_metrics = {}
    total_impact = 0
    competitive_positions = []
    
    for analysis in analyses:
        all_segments.update(analysis.get('user_segments', []))
        
        # Aggregate metrics
        for metric in analysis.get('affected_metrics', []):
            metric_name = metric.get('metric', '')
            if metric_name not in all_metrics:
                all_metrics[metric_name] = metric
            else:
                existing = all_metrics[metric_name]
                # Update with higher confidence values
                if metric.get('confidence', 0) > existing.get('confidence', 0):
                    existing.update(metric)
        
        # Track impact scores
        impact_scores = {'low': 1, 'medium': 2, 'high': 3}
        total_impact += impact_scores.get(analysis.get('potential_impact', 'low'), 1)
        
        if analysis.get('competitive_position'):
            competitive_positions.append(analysis['competitive_position'])
    
    # Calculate average impact
    avg_impact = total_impact / (len(analyses) or 1)
    impact = 'high' if avg_impact > 2 else 'medium' if avg_impact > 1 else 'low'
    
    # Choose most detailed competitive position
    comp_pos = max(competitive_positions, key=lambda x: len(str(x))) if competitive_positions else {
        'current_state': 'unknown',
        'potential_advantage': 'undefined',
        'competitor_comparison': []
    }
    
    return ImpactAnalysis(
        user_segments=list(all_segments),
        affected_metrics=[ImpactMetrics(**metric) for metric in all_metrics.values()],
        potential_impact=impact,
        competitive_position=CompetitivePosition(**comp_pos)
    )

def aggregate_opportunities(opportunities_list: List[Dict]) -> List[MarketOpportunity]:
    """Aggregate opportunities across reviews."""
    opportunities_map = {}
    
    for opportunities in opportunities_list:
        for opp in opportunities:
            opp_id = opp.get('id')
            if opp_id not in opportunities_map:
                opportunities_map[opp_id] = opp
            else:
                existing = opportunities_map[opp_id]
                # Update with higher confidence values
                if opp.get('confidence_score', 0) > existing.get('confidence_score', 0):
                    # Aggregate evidence
                    existing['evidence'] = aggregate_evidence([
                        existing.get('evidence', {}),
                        opp.get('evidence', {})
                    ]).__dict__
                    # Aggregate implementation
                    existing['implementation'] = aggregate_implementation([
                        existing.get('implementation', {}),
                        opp.get('implementation', {})
                    ]).__dict__
                    # Aggregate impact analysis
                    existing['impact_analysis'] = aggregate_impact_analysis([
                        existing.get('impact_analysis', {}),
                        opp.get('impact_analysis', {})
                    ]).__dict__
                    # Update confidence score
                    existing['confidence_score'] = opp['confidence_score']

    return [MarketOpportunity(**opp) for opp in opportunities_map.values()]

def summarize_opportunities_for_date(date: date, app_id: str) -> OpportunitiesAnalysis:
    """Summarize opportunities analysis for a specific date and app."""
    reviews = get_reviews_with_opportunities(date, app_id)
    logger.info(f"Processing opportunities analysis for {len(reviews)} reviews")

    if not reviews:
        return OpportunitiesAnalysis(
            analysis_error="No reviews found for the specified date",
            market_opportunities=[]
        )

    all_opportunities = []

    for review in reviews:
        opportunities_data = review.get('opportunities')
        if not opportunities_data:
            continue

        # Parse JSON if needed
        if isinstance(opportunities_data, str):
            try:
                import json
                opportunities_data = json.loads(opportunities_data)
            except json.JSONDecodeError:
                logger.error("Failed to parse opportunities JSON")
                continue

        # Extract opportunities
        opportunities = opportunities_data.get('market_opportunities', [])
        if opportunities:
            all_opportunities.append(opportunities)

    # Aggregate opportunities
    market_opportunities = aggregate_opportunities(all_opportunities)

    # Return final analysis
    return OpportunitiesAnalysis(
        market_opportunities=market_opportunities
    )

if __name__ == "__main__":
    test_date = date(2025, 3, 11)
    test_app_id = "com.kcb.mobilebanking.android.mbp"
    
    opportunities = summarize_opportunities_for_date(test_date, test_app_id)
    if opportunities:
        logger.info("Opportunities Analysis Summary:")
        logger.info(f"Market opportunities found: {len(opportunities.market_opportunities)}")
        
        for opp in opportunities.market_opportunities:
            logger.info(f"\nOpportunity {opp.id}:")
            logger.info(f"Title: {opp.title}")
            logger.info(f"Type: {opp.type}")
            logger.info(f"Confidence Score: {opp.confidence_score:.2f}")
            logger.info(f"Description: {opp.description}")
            
            logger.info("\nEvidence:")
            logger.info(f"Review Count: {opp.evidence.review_count}")
            logger.info(f"User Sentiment: {opp.evidence.user_sentiment:.2f}")
            logger.info(f"Mention Frequency: {opp.evidence.mention_frequency}")
            
            logger.info("\nImplementation:")
            logger.info(f"Complexity: {opp.implementation.complexity}")
            logger.info(f"Timeline: {opp.implementation.estimated_timeline}")
            logger.info(f"Dependencies: {', '.join(opp.implementation.dependencies)}")
            
            logger.info("\nImpact Analysis:")
            logger.info(f"Potential Impact: {opp.impact_analysis.potential_impact}")
            logger.info(f"User Segments: {', '.join(opp.impact_analysis.user_segments)}")
    else:
        logger.warning("No opportunities analysis available for the specified date") 