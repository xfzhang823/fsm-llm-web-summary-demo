"""Prompt bank for standard prompt template/placeholders"""

# prompt_bank.py

CLEAN_JOB_PAGE_PROMPT = """
Task: Filter job posting content to retain only the following elements:
- About Us / Company descriptions
- Job titles and descriptions
- Core responsibilities
- Required qualifications
- Benefits and compensation
- Company details and information

Content: {content}

Instructions:
- Remove all content not related to the categories above
- Return only the filtered text
- Do not include explanations or annotations

Format: Plain text output
"""


# !You need to use Escaping Curly Braces, "{{ }}", for JSON output examples!
# !When use .format() method in Python strings, any curly braces {} in the string
# !are treated as placeholders for variable substitution.
# !Therefore, you need to escape them by doubling them ({{ for { and }} for })!!!
# Updated this with Claude's suggested prompt (generally better than other LLMs)
CONVERT_JOB_POSTING_TO_JSON_PROMPT = """
You are a skilled professional at analyzing job descriptions. Extract key information and relevant content \
from the provided job description, converting it into a comprehensive, valid JSON format. \
Exclude purely website-related information or general company boilerplate not specific to the job.

Instructions:
1. Identify and extract the following key details if present:
   - url
   - Job title
   - Company name
   - Location
   - Salary information
   - Posted date (if available)
2. Capture the job-specific content, preserving its structure as much as possible.
3. Use clear section headers as keys in the JSON structure when present.
4. For content without clear section headers, use general keys like "description" or "additional_info".
5. Exclude information that is:
   - Purely related to website navigation or functionality
   - Generic company information not specific to the job role
   - Legal disclaimers or privacy policies unrelated to the job
   - Repetitive footer information

Content to process: {content}

Output structure:
{{
  "url": "<string or null if not found>",
  "job_title": "<string or null if not found>",
  "company": "<string or null if not found>",
  "location": "<string or null if not found>",
  "salary_info": "<string or null if not found>",
  "posted_date": "<string or null if not found>",
  "content": {{
    "<section_name>": "<full text of relevant section>",
    "<another_section>": "<full text of relevant section>",
    "description": "<any relevant content without a clear section header>",
    "additional_info": "<any remaining relevant content>"
  }}
}}

JSON Formatting Instructions:
1. Use "null" (without quotes) for any key details not found in the content.
2. Preserve newlines within string values using \\n.
3. Escape any double quotes within string values.
4. Ensure the output is a valid JSON object.

Extract all job-relevant information, but exclude website-specific content and generic company information not directly related to the job posting.
"""

EXTRACT_JOB_REQUIREMENTS_PROMPT = """
You are a job description analysis expert. Extract and organize **explicit** and **implied** job requirements \
from the following job description. Group similar responsibilities into concise themes to avoid excessive bullets.

### **Instructions:**
1️. **Extract Key Employer Expectations**  
   - Identify requirements from sections like 'Qualifications,' 'Requirements,' 'Responsibilities,' 'Skills,' 'Experience,' 'What Will You Do' and similar.  
   - Include both **explicit** qualifications (stated clearly) and **implied** qualifications (inferred from responsibilities).  
   - Summarize responsibilities into **high-level themes** rather than listing every detail.

2️. **Categorize into Five Tiers:**  
   - **pie_in_the_sky**: Ambitious, high-level expectations (e.g., "Led multi-billion dollar transformations").  
   - **down_to_earth**: Practical, commonly expected qualifications (e.g., "5+ years of business development experience").  
   - **bare_minimum**: Essential, non-negotiable requirements (e.g., "Bachelor’s degree in marketing").  
   - **cultural_fit**: Soft skills, values, and leadership qualities (e.g., "Collaborative, growth-oriented mindset").  
   - **other**: Specific tools, certifications, or unique qualifications (e.g., "Expertise in Salesforce CRM").

3️. **Group Responsibilities into Concise, Meaningful Insights If Possible**  
   - Instead of listing **every** responsibility separately, **group similar responsibilities** into **high-level summaries**.  
   - Example:
     ✅ "Develop and execute enterprise sales strategies, manage key client relationships, and drive revenue growth."`
     ❌ "Develop enterprise sales strategies."`, `"Manage client relationships."`, `"Drive revenue growth."` *(Too granular!)*  

4️. **Limit Output Size**  
   - ⚡ **Maximum 5-7 key points per category** (Choose the most representative ones).
   - 🚀 **Avoid redundancy** (Don't repeat qualifications across categories).  


### **Job Description to Analyze:**
{content}


### **Expected Output Format (JSON):**
{{
  "pie_in_the_sky": [
    "Proven success leading large-scale business growth initiatives at the enterprise level.",
    "Track record of driving multi-million dollar revenue increases through strategic market expansion."
  ],
  "down_to_earth": [
    "5+ years of experience in business development, client relationship management, or marketing.",
    "Proficiency in CRM tools such as Salesforce for tracking and analyzing client interactions."
  ],
  "bare_minimum": [
    "Bachelor’s degree in marketing, business, or a related field.",
    "Strong verbal and written communication skills."
  ],
  "cultural_fit": [
    "Collaborative leadership style with a growth-oriented mindset.",
    "Ability to mentor and develop high-performing teams."
  ],
  "other": [
    "Experience working in professional services, Big Four firms, or consulting environments."
  ]
}}
"""

SEMANTIC_ENTAILMENT_ALIGNMENT_PROMPT = """
You are a skilled professional at writing resumes. Please perform the following tasks:
1. Optimize the candidate text to increase semantic precision and similarity with the reference text.
2. Reduce the semantic distance between the candidate text and reference text.
3. Enhance entailment relationships, where the candidate text serves as the premise and the reference text serves as the hypothesis.
4. For the entailment task, the candidate text serves as the premise and the reference text serves as the hypothesis, reversing their typical roles.
5. Improve the overall alignment and relevance between the two texts, without compromising their directional relationship.
6. Do not copy specific experience durations directly from the reference text (e.g., "11 years experience in...", "more than 10 years in...", "8 years experience"). Instead, express relevant experience in more general terms or use ranges that encompass the candidate's actual experience.

**Candidate text:**
"{content_1}"

**Reference text:**
"{content_2}"

**Return Format:**
Return only a valid JSON object, without markdown formatting, as follows:

{{
  "optimized_text": "Edited version of candidate text"
}}

Return only the JSON block without any additional text, explanations, or markdown syntax.
"""

ENTAILMENT_ALIGNMENT_PROMPT = """
You are a skilled professional at writing resumes. Please perform the following tasks:
1. Modify the **premise text** to enhance its entailment relationship with the **hypothesis text**.
2. Improve the overall alignment and relevance between the two texts without changing the directional relationship.
3. Do not copy specific experience durations directly from the reference text (e.g., "11 years experience in...", "more than 10 years in...", "8 years experience"). Instead, express relevant experience in more general terms or use ranges that encompass the candidate's actual experience.
4. Do not copy specific experience durations directly from the reference text (e.g., "11 years experience in...", "more than 10 years in...", "8 years experience"). Instead, express relevant experience in more general terms or use ranges that encompass the candidate's actual experience.

**Premise text:**
"{content_1}"

**Hypothesis text:**
"{content_2}"

**Return Format:**
Return only a valid JSON object, without markdown formatting, as follows:

{{
  "optimized_text": "Edited version of premise text"
}}

Return only the JSON block without any additional text, explanations, or markdown syntax.
"""


SEMANTIC_ALIGNMENT_PROMPT = """
You are a skilled professional at writing resumes. Please perform the following tasks:
1. Optimize the **candidate text** to increase semantic precision and similarity with the **reference text**.
2. Reduce the semantic distance between them.
3. Do not copy specific experience durations directly from the reference text (e.g., "11 years experience in...", "more than 10 years in...", "8 years experience"). Instead, express relevant experience in more general terms or use ranges that encompass the candidate's actual experience.

**Candidate text:**
"{content_1}"

**Reference text:**
"{content_2}"

**Return Format:**
Return only a valid JSON object, without markdown formatting, as follows:

{{
  "optimized_text": "Edited version of candidate text"
}}

Return only the JSON block without any additional text, explanations, or markdown syntax.
"""


STRUCTURE_TRANSFER_PROMPT = """
You are a skilled professional at writing resumes. Please perform the following tasks:
1. Analyze the **source text** at a high level.
2. Apply the **source text's** sentence structure or syntactic dependencies to the **target text**, ensuring \
that the original meaning of the **target text** is preserved as much as possible.
3. If the source text has an active tone, retain it. Use strong action verbs such as 'led,' 'managed,' 'developed,' \
'executed,' etc., and explicitly avoid role-based constructions like 'professional who is,' 'was involved in,' or \
'held a position of.' Instead, focus on action-based descriptions that highlight what was achieved or done, \
such as 'led a team,' 'implemented strategies,' or 'drove improvements.'
3. Do not copy specific experience durations directly from the reference text \
(e.g., "11 years experience in...", "more than 10 years in...", "8 years experience"). \
Instead, express relevant experience in more general terms or use ranges that encompass the candidate's \
actual experience.

**Target Text:**
"{content_1}"

**Source Text:**
"{content_2}"

**Return Format:**
Return only a valid JSON object, without markdown formatting, as follows:

{{
  "optimized_text": "Edited version of the target text"
}}

Return only the JSON block without any additional text, explanations, or markdown syntax.
"""

# Create a prompt and insert the JSON data into it
MATCHING_KEYWORDS_AGAINST_TEXT_PROMPT = """
You are a skilled professional at writing resumes. Please perform the following tasks:
1. Given the following **resume keywords** in JSON, extract keywords from the {content_1} sections.
2. Match these extracted keywords with the **job description** in JSON provided below, replace or modify \
the **resume keywords** if they are similar to **job description keywords**, and then prioritize them based on \
their importance to the job description.

**Resume keywords** in JSON:
{content_2}

**Job description** in JSON:
{content_3}

**Return Format:**
Return only a valid JSON object, without markdown formatting, as follows:

{{
  "optimized_keywords": [],
  "prioritized_keywords": []
}}

Return only the JSON block without any additional text, explanations, or markdown syntax.
"""

# # Insert the JSON object as a string into the prompt
# formatted_prompt = prompt_template.format(resume_json=json.dumps(json_data, indent=2))

TRIM_CONDENSE_FINAL_RESUME_PROMPT = """
You are an expert resume optimizer for senior-level tech and consulting roles.
Your task is to **trim, consolidate, and optimize** the provided JSON resume data while maintaining clarity, impact, and ATS compatibility.

### **Task Instructions**
1. **Trim & Condense:** Reduce the word count to approximately **500-600 words** while retaining essential details.
2. **Merge Related Content:** Combine overlapping or redundant bullets to enhance readability and conciseness.
3. **Enhance ATS Compliance:** Ensure the use of **strong action verbs**, **industry-specific keywords**, and **quantifiable achievements** \
(e.g., "% improvements," "increased revenue by X%," "cut processing time by Y%").
4. **Ensure Consistency & Clarity:** Maintain a **structured, professional format**, ensuring smooth readability while preserving \
key impact points.
5. **Strict JSON Output:** Format the output **identically to the input JSON structure**, preserving the original schema.

---
### **Input Resume in JSON Format**
{resume_json}

---
### **Expected Output (Optimized JSON Format)**
- The **same JSON structure** as the input.
- **Trimmed, condensed, and optimized** responsibilities and achievements.
- **Strict adherence to JSON formatting** (no extra explanations, markdown, or non-JSON content).
- Output must be a **valid JSON object**.

### **Expected Output Format (JSON Example)**
{{
  "edited_responsibility_1": {
    "0.responsibilities.0": "Optimized AI-driven workflows, enhancing user engagement and commercial potential.",
    "0.responsibilities.1": "Developed scalable AI-first strategies, integrating NLP and ML models to drive efficiency.",
    ...
  }
}}

Return only the JSON block without any additional text, explanations, or markdown syntax.
"""

TRIM_CONDENSE_FINAL_RESUME_JSON_TO_TEXT_PROMPT = """
You are a text-cleaning assistant. I will give you a JSON object.

TASK:
1. Read all keys and values.
2. Rewrite or condense values for clarity, professionalism, and impact.
3. Merge or simplify overlapping bullets; use strong action verbs and relevant industry keywords.
4. Trim the total wording to a concise, readable form (approx. 500–600 words if long).
5. Convert lists into clean bullet lists.
6. Do NOT modify or remove keys—only rewrite the values.
7. Improve flow, readability, and parallel structure.

RETURN FORMAT:
- Return **plain text only**, not JSON.
- Each key on its own line followed by a colon.
- Cleaned value on the next line.
- Double-space between entries.
- No quotes, no backticks, no markdown.

INPUT JSON:
{{INSERT_JSON_HERE}}
"""


TRIM_CONDENSE_FINAL_RESUME_WITH_JOB_DESC_PROMPT = """
You are an expert resume optimizer for senior-level tech and consulting roles.
Your task is to **trim, consolidate, and optimize** the provided JSON resume data while ensuring alignment \
with the job description provided.

### **Task Instructions**
1. **Trim & Condense:** Reduce the resume word count to approximately **500-600 words** while retaining essential details.
2. **Merge Related Content:** Combine overlapping or redundant bullets to enhance readability and conciseness.
3. **Enhance ATS Compliance:** Ensure the use of **strong action verbs**, **industry-specific keywords**, and **quantifiable achievements** \
(e.g., "% improvements," "increased revenue by X%," "cut processing time by Y%").
4. **Align with Job Description:** Ensure the resume highlights **key skills, experience, and qualifications** relevant to the job description.
5. **Ensure Consistency & Clarity:** Maintain a **structured, professional format**, ensuring smooth readability while preserving key impact points.
6. **Strict JSON Output:** Format the output **identically to the input JSON structure**, preserving the original schema.

---
### **Input Resume in JSON Format**
{resume_json}

---
### **Input Job Description in JSON Format**
{job_description_json}

---
### **Expected Output (Optimized JSON Format)**
- The **same JSON structure** as the input.
- **Trimmed, condensed, and optimized** responsibilities and achievements.
- **Alignment with job description** to enhance relevance.
- **Strict adherence to JSON formatting** (no extra explanations, markdown, or non-JSON content).
- Output must be a **valid JSON object**.

### **Expected Output Format (JSON Example)**
{{
  "edited_responsibility_1": {{
    "0.responsibilities.0": "Optimized AI-driven workflows, enhancing user engagement and commercial potential.",
    "0.responsibilities.1": "Developed scalable AI-first strategies, integrating NLP and ML models to drive efficiency.",
    ...
  }}
}}

Return only the JSON block without any additional text, explanations, or markdown syntax. 
"""

# Todo: for later; do not use it now!
MODIFYING_FINAL_RESUME_PROMPT = """
You are an expert in ATS (Applicant Tracking System) optimization. 
Your task is to refine the provided JSON resume data with a light touch to emphasize relevant \
content and maximize ATS compatibility for the specified job role.

## Input
- JSON-formatted resume data {resume_json}
- JSON-formatted job description {job_description}
- JSON-formatted list of keywords {keywords}
- Target role name {target_role}
- Target company name {target_company}

<Instructions>
1. Maintain the existing content structure and most of the original wording
2. Trim & Condense: Reduce the word count to approximately 500-600 words while retaining essential details
3. Merge Related Content: Combine overlapping or redundant bullets to enhance readability and conciseness
4. Highlight relevant experience by:
   - Ensuring important achievements and skills appear early in each section
   - Adding ATS-friendly keywords naturally where they fit existing content
5. Improve ATS compatibility by:
   - Using standard industry terminology that matches the job description
   - Replacing passive language with active, action-oriented verbs
   - Ensuring quantifiable metrics and achievements are clearly visible
6. Do not:
   - Add new experiences or significantly alter the meaning of existing content
   - Force keywords where they don't naturally fit

## Expected Output
Return your ATS-optimized resume as a JSON object named 'optimized_resume_json' that maintains \
the same structure as the input resume, as followings:
{{
  "optimized_resume_json": {{
    "0.responsibilities.0": "Led cross-functional team of 12 engineers in developing scalable data platform...",
    "0.responsibilities.1": "Architected and implemented cloud-based AI solution...",
    ...
  }}
}}

Return only the JSON block without any additional text, explanations, or markdown syntax.
"""
