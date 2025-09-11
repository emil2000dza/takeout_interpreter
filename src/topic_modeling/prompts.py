TOPIC_DISCOVERY_PROMPT = """
You are an expert in topic modeling and semantic clustering. 
You are given a collection of Chrome browsing history entries on {domain} website. 
Each entry has:
- title: the page title
- url: the full URL visited

## Data sample:
{history_sample}

## Your task:
1. Analyze the browsing history on {domain} to discover recurring **topics**.
2. Consider:
   - The reputation and main purpose of {domain}.
   - The type of content searched or viewed.
   - The user's apparent intent based on the title and URL visited.
3. Group similar entries into topics that are **general enough** to include future related pages, 
   but **specific enough** to be meaningful.
4. Avoid making a topic that’s only a single URL.
5. Prioritize topics that are **actionable** or relevant to productivity analysis.
6. Do not output more than 10 topics. 

## Output:
You must output **only valid JSON**, with **double quotes**, no trailing commas, no extra text. 
Do not add any explanations or commentary. In the valid JSON object each key is a topic 
name and each value is:
- "description": short explanation of the topic.
- "example_domains": list of representative domains.
- "example_titles": list of example titles.

## Example:
{{
  "Topic Name": {{
    "description": "short description",
    "example_domains": ["domain1.com/a", "domain1.com/b"],
    "example_titles": ["Title 1", "Title 2"]
  }}
}}

Now, extract the topic set from the {domain} browsing history.
"""

TOPIC_REFINMENT_PROMPT = """
You are an expert in profiling and semantic clustering. 
You are given a collection of general topics from Chrome browsing history on {domain} website of a specific user.
Your goal is to interpret the user history and maximizes the actions he could take regarding its online
activity on {domain}. Goal is to give specific and actionnable insights to him/her.
Each entry has:
- name: the topic name, a comprehensive title specifying a part of the browsing activity of our user
- description: a more complete description of the topic and how it is related to the browsing activity

## Data:
{all_topics}

## Your task:
1. Analyze the recurring topics regarding the browsing activity on {domain} and refine it to 5 to 15 broader 
topics that would particularly interest the user on his activity.
2. Be explicit in the title and give more detail in the description, 
3. Make sure in the wording you use that the user discovers himself through the analysis of the output topics,
3. Refined topics should be **general enough** to include future related pages, 
   but **specific enough** to be meaningful.
5. Prioritize topics that are **actionable** or relevant to productivity analysis.
6. Do not output more than 15 topics. 

## Output:
You must output **only valid JSON**, with **double quotes**, no trailing commas, no extra text. 
Do not add any explanations or commentary. In the valid JSON object each key is a topic 
name and each value is:
- "description": short explanation of the topic.
- "example_domains": list of representative domains.
- "example_titles": list of example titles.

## Example:
{{
  "Topic Name": {{
    "description": "short description",
    "example_domains": ["domain1.com/a", "domain1.com/b"],
    "example_titles": ["Title 1", "Title 2"]
  }}
}}

Now, extract the topic set from the {domain} browsing history.
"""

TOPIC_ASSIGNMENT_PROMPT = """
You are a classification system that assigns browsing history entries in {domain} website to predefined topics.

## Topics:
{topics_json} 

Each topic has:
- description: explanation of what it includes

## Browsing history entry:
title: {title}
url: {url}

## Task:
- Select the relevant topics (max. 3 topics) associated to the entry from the topic list.
- If none applies perfectly,  output "Other".
- If the URL and title can not lead to any actionnable insight for the user output an empty JSON,
- Consider the specific page content browsed an that the user browses in {domain}.

## Output:
You must output **only valid JSON**, with **double quotes**, no trailing commas, no extra text. 
Do not add any explanations or commentary. In the valid JSON object each key is a topic 
name and each value is:
- "reasoning": short explanation of the toassociation of the URL tp the topic.
{{
  "Topic Name": {{
    "reasoning": "short explanation",
  }}
}}
"""

BATCH_TOPIC_ASSIGNMENT_PROMPT = """
You are a strict JSON classification system. Your role is to assign batches of browsing history entries on {domain} website to one or more predefined topics.

## Topics:
{topics_json} 

Each topic has:
- "name": the topic label
- "description": explanation of what it includes

## Batch of browsing history entries:
{urls}

## Task:
For each browsing history entry in the batch:
1. Match the entry to **up to 3 relevant topics** from the topic list above.
   - Select topics that best reflect the page intent in the title/URL given {domain} website reputation.
2. If no listed topic applies, assign the entry to **"Other"**.
3. If the entry contains no actionable insight (e.g., a meaningless URL and title combo), return **no object** for that entry (classes should be empty).

## Output rules (critical):
- Return **only one valid JSON object**.
- ** INCLUDE EVRY URL OF THE INPUT IN THE RESULTING JSON**,
- Each key must be the **URL** of the entry.
- Each value must be an object with one key `"classes"` mapped to an array of topic names.
- Always use **double quotes**, no trailing commas, no commentary, no explanations.

## Valid Output Format Example:
{{
  "http://example1.com/page": {{
    "classes": ["Topic A", "Topic B"]
  }},
  "http://example2.org/resource": {{
    "classes": ["Other"]
  }}
}}
"""
