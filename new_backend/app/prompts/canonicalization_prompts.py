from app.models.canonicalization_models import llm_input, llm_output, Category, Subcategory

def canonization_with_examples(llm_input: llm_input) -> str:
    """Generate comprehensive prompt for canonicalization when examples are provided."""
    
    # Get all available categories and subcategories
    available_categories = [cat.value for cat in Category]
    available_subcategories = [sub.value for sub in Subcategory]
    
    return f"""
You are an expert canonicalization assistant that maps free-text statements to standardized canonical IDs. Your goal is to either:
1. Choose the BEST matching canonical ID from the provided candidates, OR
2. Create a NEW canonical ID if none of the candidates are suitable

## INPUT STATEMENT:
{llm_input.statement}

## ENRICHED CANDIDATES:
{llm_input.enriched_candidates}

## AVAILABLE CATEGORIES (Choose from these or create new):
{', '.join(available_categories)}

## AVAILABLE SUBCATEGORIES (Choose from these or create new):
{', '.join(available_subcategories)}

## INSTRUCTIONS:

### Step 1: Analyze the Input Statement
- Understand the core meaning and intent
- Identify the main topic (performance, UI, authentication, etc.)
- Consider the sentiment (positive, negative, neutral)

### Step 2: Evaluate Each Candidate
For each candidate, consider:
- **Semantic Similarity**: How well does the candidate's meaning match the input?
- **Context Relevance**: Is the candidate in the right category/context?
- **Specificity**: Is the candidate too broad or too narrow?
- **Confidence Score**: Higher scores indicate better matches

### Step 3: Decision Making
- **If a candidate is a PERFECT or VERY GOOD match** → Choose that canonical_id
- **If candidates are PARTIAL matches** → Choose the best one and explain why
- **If NO candidates are suitable** → Create a new canonical_id

### Step 4: Creating New Canonical IDs (if needed)
If creating a new ID, follow these rules:
- Use snake_case format (lowercase with underscores)
- Be descriptive but concise (3-5 words max)
- Focus on the core concept
- Examples: `slow_transactions`, `confusing_ui`, `duplicate_charges`

### Step 5: Category and Subcategory Selection
When creating new canonical IDs, you MUST:
1. **Choose from existing categories** if one fits well
2. **Create a new category** if none of the existing ones match
3. **Choose from existing subcategories** if one fits well
4. **Create a new subcategory** if none of the existing ones match
5. **Provide reasoning** for your category/subcategory choices

## EXAMPLES:

### Example 1: Perfect Match
**Input**: "The app is very slow when processing payments"
**Candidates**: [{{"canonical_id": "slow_transactions", "display_label": "Slow Transaction Processing", "description": "Payment processing takes too long", "score": 0.95}}]
**Output**: {{"canonical_id": "slow_transactions", "existing_canonical_id": true, "reasoning": "Perfect semantic match - both refer to slow payment processing", "error": null}}

### Example 2: Good Match with Reasoning
**Input**: "I got charged twice for the same purchase"
**Candidates**: [{{"canonical_id": "duplicate_charge", "display_label": "Duplicate Charges", "description": "User charged multiple times for single transaction", "score": 0.89}}]
**Output**: {{"canonical_id": "duplicate_charge", "existing_canonical_id": true, "reasoning": "Excellent match - 'charged twice' directly corresponds to 'duplicate charges'", "error": null}}

### Example 3: Partial Match with Selection
**Input**: "The interface is confusing and hard to navigate"
**Candidates**: [{{"canonical_id": "app_not_opening", "display_label": "App Won't Open", "description": "Application fails to launch", "score": 0.45}}, {{"canonical_id": "confusing_ui", "display_label": "Confusing User Interface", "description": "Navigation unclear; screens cluttered", "score": 0.78}}]
**Output**: {{"canonical_id": "confusing_ui", "reasoning": "Best available match - 'confusing interface' aligns with 'confusing UI', though not perfect", "error": null}}

### Example 4: No Suitable Match - Create New
**Input**: "This app is not as good as the competitor's version"
**Candidates**: [{{"canonical_id": "app_not_opening", "display_label": "App Won't Open", "description": "Application fails to launch", "score": 0.32}}]
**Output**: {{
    "canonical_id": "inferior_to_competitors",
    "existing_canonical_id": false,
    "reasoning": "No suitable match found. Creating new ID for competitive comparison feedback",
    "error": null,
    "canonical_fields": {{
        "category": "Competitive Analysis",
        "subcategory": "Market Comparison",
        "sub_subcategory1": null,
        "display_label": "Inferior to Competitors",
        "description": "User feels the app is not as good as competing applications",
        "examples": ["not as good as competitor", "worse than alternatives", "competitor is better", "inferior to other apps"],
        "aliases": ["competitor better", "other apps superior", "not competitive"]
    }}
}}

### Example 5: Reject All Candidates
**Input**: "The app crashes every time I try to upload a photo"
**Candidates**: [{{"canonical_id": "slow_transactions", "display_label": "Slow Transactions", "description": "Payment processing is slow", "score": 0.23}}]
**Output**: {{
    "canonical_id": "app_crashes_on_photo_upload",
    "existing_canonical_id": false,
    "reasoning": "None of the candidates match. Creating specific ID for photo upload crashes",
    "error": null,
    "canonical_fields": {{
        "category": "Stability",
        "subcategory": "Crash",
        "sub_subcategory1": "Photo Upload",
        "display_label": "App Crashes on Photo Upload",
        "description": "Application crashes specifically when uploading photos or images",
        "examples": ["crashes on photo upload", "freezes when uploading images", "photo upload causes crash", "app dies when adding photos"],
        "aliases": ["photo upload crash", "image upload freeze", "photo upload failure"]
    }}
}}

## OUTPUT FORMAT:
Return ONLY a JSON object with this exact structure:
{{
    "canonical_id": "the_selected_or_new_canonical_id",
    "existing_canonical_id": true_or_false,
    "reasoning": "Detailed explanation of why this ID was chosen or created",
    "error": null,
    "canonical_fields": {{
        "category": "existing_or_new_category",
        "subcategory": "existing_or_new_subcategory",
        "sub_subcategory1": "optional_sub_subcategory_or_null",
        "display_label": "Human readable label",
        "description": "Detailed description",
        "examples": ["example1", "example2", "example3", "example4"],
        "aliases": ["alias1", "alias2", "alias3"]
    }}
}}

## IMPORTANT RULES:
1. **Be Conservative**: Only create new IDs when absolutely necessary
2. **Prefer Existing**: Always prefer existing IDs if they're reasonable matches
3. **Be Specific**: New IDs should be specific to the actual issue
4. **Follow Format**: Use snake_case for all canonical IDs
5. **Explain Clearly**: Provide detailed reasoning for your choice
6. **Consider Context**: Think about the broader category and user intent
7. **Use Available Categories**: Prefer existing categories/subcategories over creating new ones
8. **Justify New Categories**: If creating new categories/subcategories, explain why existing ones don't fit

Now analyze the input statement and provide your response:
"""

def canonization_without_examples(llm_input: llm_input) -> str:
    """Generate comprehensive prompt for canonicalization when no examples are provided."""
    
    # Get all available categories and subcategories
    available_categories = [cat.value for cat in Category]
    available_subcategories = [sub.value for sub in Subcategory]
    
    return f"""
You are an expert canonicalization assistant that creates standardized canonical IDs for free-text statements. Your goal is to create a new canonical ID that accurately represents the statement's meaning.

## INPUT STATEMENT:
{llm_input.statement}

## AVAILABLE CATEGORIES (Choose from these or create new):
{', '.join(available_categories)}

## AVAILABLE SUBCATEGORIES (Choose from these or create new):
{', '.join(available_subcategories)}

## INSTRUCTIONS:

### Step 1: Analyze the Statement
- Identify the core topic and intent
- Determine the category (performance, UI, authentication, payments, etc.)
- Assess the sentiment and severity
- Consider the user's perspective and context

### Step 2: Create Canonical ID
Follow these guidelines:
- **Format**: Use snake_case (lowercase with underscores)
- **Length**: 3-5 words maximum
- **Specificity**: Be specific but not overly detailed
- **Clarity**: Make it clear what the issue/feedback is about
- **Consistency**: Follow established patterns

### Step 3: Select or Create Category
- **First**: Check if any existing category fits well
- **If yes**: Use the existing category
- **If no**: Create a new category name that's clear and descriptive
- **Explain**: Provide reasoning for your choice

### Step 4: Select or Create Subcategory
- **First**: Check if any existing subcategory fits well
- **If yes**: Use the existing subcategory
- **If no**: Create a new subcategory name that's specific and clear
- **Explain**: Provide reasoning for your choice

### Step 5: Validation
Before finalizing, ask:
- Is this ID clear and understandable?
- Would other similar statements map to this ID?
- Is it specific enough but not too narrow?
- Does it follow the naming convention?
- Are the category/subcategory choices appropriate?

## EXAMPLES OF GOOD CANONICAL IDs:

### Performance Issues:
- `slow_transactions` (for slow payment processing)
- `app_crashes_frequently` (for frequent crashes)
- `long_load_times` (for slow app loading)
- `laggy_interface` (for unresponsive UI)

### UI/UX Issues:
- `confusing_navigation` (for unclear menu structure)
- `poor_button_placement` (for bad UI layout)
- `unreadable_text` (for font/contrast issues)
- `cluttered_interface` (for too much information)

### Authentication Issues:
- `login_fails_repeatedly` (for persistent login problems)
- `password_reset_broken` (for reset functionality issues)
- `otp_not_received` (for missing verification codes)
- `account_locked_unfairly` (for wrongful account locks)

### Payment Issues:
- `duplicate_charges` (for double billing)
- `payment_fails_silently` (for failed payments without error)
- `refund_not_processed` (for missing refunds)
- `wrong_amount_charged` (for billing errors)

### Feature Issues:
- `missing_essential_feature` (for absent functionality)
- `feature_broken` (for non-working features)
- `incomplete_functionality` (for partial feature implementation)
- `feature_hard_to_find` (for discoverability issues)

## EXAMPLES:

### Example 1: Selecting Existing Canonical ID
**Input**: "The app is very slow when processing payments"
**Candidates**: [{{"canonical_id": "slow_transactions", "display_label": "Slow Transaction Processing", "description": "Payment processing takes too long", "score": 0.95}}]
**Output**: {{
    "canonical_id": "slow_transactions",
    "existing_canonical_id": true,
    "reasoning": "Perfect match - both refer to slow payment processing",
    "error": null,
    "canonical_fields": null
}}

### Example 2: Creating New Canonical ID
**Input**: "The app takes forever to load my account balance"
**Output**: {{
    "canonical_id": "slow_balance_loading",
    "existing_canonical_id": false,
    "reasoning": "Specific performance issue related to account balance display",
    "error": null,
    "canonical_fields": {{
        "category": "Performance",
        "subcategory": "Processing",
        "sub_subcategory1": "Balance Display",
        "display_label": "Slow Balance Loading",
        "description": "Account balance takes too long to load and display to the user",
        "examples": ["balance loading slow", "takes forever to show balance", "balance display delay", "account balance slow to load"],
        "aliases": ["slow balance", "balance loading delay", "account balance delay"]
    }}
}}

### Example 2: UI Issue
**Input**: "I can't find the settings menu anywhere"
**Output**: {{
    "canonical_id": "settings_hard_to_find",
    "existing_canonical_id": false,
    "reasoning": "Navigation/discoverability issue with settings menu",
    "error": null,
    "canonical_fields": {{
        "category": "UX",
        "subcategory": "Navigation",
        "sub_subcategory1": "Menu Discovery",
        "display_label": "Settings Hard to Find",
        "description": "Settings menu is difficult to locate or discover within the app",
        "examples": ["can't find settings", "settings menu hidden", "settings hard to locate", "where are the settings"],
        "aliases": ["settings hidden", "settings menu missing", "settings not found"]
    }}
}}

### Example 3: Payment Issue
**Input**: "I was charged $50 but the receipt shows $45"
**Output**: {{
    "canonical_id": "billing_amount_mismatch",
    "existing_canonical_id": false,
    "reasoning": "Discrepancy between charged amount and receipt",
    "error": null,
    "canonical_fields": {{
        "category": "Payments",
        "subcategory": "Fees",
        "sub_subcategory1": "Billing Discrepancy",
        "display_label": "Billing Amount Mismatch",
        "description": "Charged amount differs from receipt or expected amount",
        "examples": ["wrong amount charged", "receipt shows different amount", "billing discrepancy", "charged more than receipt"],
        "aliases": ["wrong billing", "amount mismatch", "receipt wrong"]
    }}
}}

### Example 4: Feature Issue
**Input**: "The search function doesn't work at all"
**Output**: {{
    "canonical_id": "search_function_broken",
    "existing_canonical_id": false,
    "reasoning": "Core search functionality is completely non-functional",
    "error": null,
    "canonical_fields": {{
        "category": "Feature",
        "subcategory": "Gap",
        "sub_subcategory1": "Search Functionality",
        "display_label": "Search Function Broken",
        "description": "Search functionality is completely non-working or broken",
        "examples": ["search doesn't work", "search function broken", "can't search anything", "search feature dead"],
        "aliases": ["search broken", "search not working", "search feature broken"]
    }}
}}

### Example 5: General Feedback
**Input**: "This app is not as good as the competitor's version"
**Output**: {{
    "canonical_id": "inferior_to_competitors",
    "existing_canonical_id": false,
    "reasoning": "General competitive comparison indicating the app is worse than alternatives",
    "error": null,
    "canonical_fields": {{
        "category": "Competitive Analysis",
        "subcategory": "Market Comparison",
        "sub_subcategory1": null,
        "display_label": "Inferior to Competitors",
        "description": "User feels the app is not as good as competing applications",
        "examples": ["not as good as competitor", "worse than alternatives", "competitor is better", "inferior to other apps"],
        "aliases": ["competitor better", "other apps superior", "not competitive"]
    }}
}}

## OUTPUT FORMAT:
Return ONLY a JSON object with this exact structure:
{{
    "canonical_id": "the_new_canonical_id",
    "existing_canonical_id": false,
    "reasoning": "Detailed explanation of why this ID was created and what it represents",
    "error": null,
    "canonical_fields": {{
        "category": "existing_or_new_category",
        "subcategory": "existing_or_new_subcategory",
        "sub_subcategory1": "optional_sub_subcategory_or_null",
        "display_label": "Human readable label",
        "description": "Detailed description",
        "examples": ["example1", "example2", "example3", "example4"],
        "aliases": ["alias1", "alias2", "alias3"]
    }}
}}

## IMPORTANT RULES:
1. **Be Specific**: Create IDs that capture the essence of the issue
2. **Be Consistent**: Follow established naming patterns
3. **Be Clear**: Make the ID self-explanatory
4. **Be Concise**: Keep it short but descriptive
5. **Consider Reusability**: Think if similar statements would use this ID
6. **Avoid Ambiguity**: Make sure the ID is unambiguous
7. **Use Available Categories**: Prefer existing categories/subcategories over creating new ones
8. **Justify New Categories**: If creating new categories/subcategories, explain why existing ones don't fit

Now analyze the input statement and create an appropriate canonical ID:
"""






