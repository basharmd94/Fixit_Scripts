import pandas as pd
import requests
import time
import re
from tqdm import tqdm

# =========================================
# OLLAMA CONFIG
# =========================================
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "gemma3:4b"

# =========================================
# FILE CONFIG
# =========================================
INPUT_FILE = "categories.xlsx"
OUTPUT_FILE = "categories_updated.xlsx"

# =========================================
# LOAD EXCEL
# =========================================
df = pd.read_excel(INPUT_FILE, dtype=str)

df = df.fillna("")

# Ensure columns exist
if "new_desc" not in df.columns:
    df["new_desc"] = ""

df["description"] = df["description"].astype(str)
df["new_desc"] = df["new_desc"].astype(str)

# =========================================
# PARENT LOOKUP
# =========================================
parent_lookup = df.set_index("id")["name"].to_dict()


def get_parent_name(parent_id):
    return parent_lookup.get(str(parent_id), "")


# =========================================
# PROMPT FOR BLANK DESCRIPTION
# =========================================
def build_prompt_for_blank(name, parent):

    return f"""
You are writing ecommerce category content for fixit.com.bd.

Write a clean SEO-friendly HTML category description.

Category: {name}
Parent: {parent}

Requirements:
- 70 to 120 words
- Natural human tone
- Mention Bangladesh naturally
- Ecommerce style writing
- Include ONE natural backlink:
<a href="https://www.fixit.com.bd">Fixit</a>
- Output HTML only
- No explanation
- No intro text
- No markdown
- Start directly with HTML

Good output example:
<p>...</p>
"""


# =========================================
# PROMPT FOR EXISTING DESCRIPTION
# =========================================
def build_prompt_for_refinement(existing_desc, name, parent):

    return f"""
You are improving ecommerce SEO content for fixit.com.bd.

Rewrite and improve the following HTML description.

Category: {name}
Parent: {parent}

Requirements:
- Keep existing HTML structure if possible
- Improve readability
- Improve SEO naturally
- Keep human tone
- Output ONLY HTML
- No explanation
- No intro text
- No markdown
- Start directly with HTML

HTML:
{existing_desc}
"""


# =========================================
# CLEAN MODEL OUTPUT
# =========================================
def clean_output(text):

    if not text:
        return ""

    # Remove think blocks
    text = re.sub(
        r"<think>.*?</think>",
        "",
        text,
        flags=re.S | re.I
    )

    # Remove markdown
    text = text.replace("```html", "")
    text = text.replace("```", "")

    # Remove common AI intro text
    bad_prefixes = [
        "Here’s a category description",
        "Here is a category description",
        "Certainly!",
        "Sure!",
        "Below is",
        "Here's the improved",
        "Here’s the improved",
    ]

    for prefix in bad_prefixes:

        if text.lower().startswith(prefix.lower()):

            split_html = re.search(r"<[a-zA-Z]", text)

            if split_html:
                text = text[split_html.start():]

    # Remove extra spaces
    text = text.replace("\n", " ")
    text = re.sub(r"\s+", " ", text)

    return text.strip()


# =========================================
# GENERATE DESCRIPTION
# =========================================
def generate_description(prompt):

    try:

        response = requests.post(
            OLLAMA_URL,
            json={
                "model": MODEL,
                "prompt": prompt,
                "stream": False,
                "temperature": 0.3,
                "top_p": 0.9,
                "num_predict": 220
            },
            timeout=180
        )

        response.raise_for_status()

        data = response.json()

        text = data.get("response", "")

        return clean_output(text)

    except Exception as e:

        print("OLLAMA ERROR:", e)

        return ""


# =========================================
# MAIN LOOP
# =========================================
updated = 0
errors = 0

for idx, row in tqdm(df.iterrows(), total=len(df)):

    try:

        name = str(row.get("name", "")).strip()

        if not name:
            continue

        if name.lower() == "uncategorized":
            continue

        parent = get_parent_name(row.get("parent", ""))

        existing_desc = str(
            row.get("description", "")
        ).strip()

        print(f"\nOptimizing: {name}")

        # =========================================
        # BUILD PROMPT
        # =========================================
        if existing_desc:

            prompt = build_prompt_for_refinement(
                existing_desc,
                name,
                parent
            )

        else:

            prompt = build_prompt_for_blank(
                name,
                parent
            )

        # =========================================
        # GENERATE
        # =========================================
        result = generate_description(prompt)

        print("\nRAW RESPONSE:")
        print(result[:400])

        # =========================================
        # VALIDATION
        # =========================================
        plain_text = re.sub(
            r"<.*?>",
            "",
            result
        ).strip()

        # Too short
        if len(plain_text) < 40:

            print("INVALID RESPONSE")

            errors += 1

            continue

        # Reject useless outputs
        bad_outputs = [
            name.lower(),
            parent.lower(),
            f"<p>{name.lower()}</p>",
            f"<p>{parent.lower()}</p>",
        ]

        if result.lower().strip() in bad_outputs:

            print("BAD OUTPUT")

            errors += 1

            continue

        # Must contain HTML
        if "<" not in result or ">" not in result:

            print("NO HTML DETECTED")

            errors += 1

            continue

        # =========================================
        # SAVE TO new_desc COLUMN
        # =========================================
        df.loc[idx, "new_desc"] = result

        updated += 1

        print(f"UPDATED: {name}")

        # =========================================
        # SMALL DELAY
        # =========================================
        time.sleep(0.4)

    except Exception as e:

        print(f"ERROR ON {name}: {e}")

        errors += 1


# =========================================
# EXPORT
# =========================================
df.to_excel(OUTPUT_FILE, index=False)

# =========================================
# DONE
# =========================================
print("\n===================================")
print("SEO OPTIMIZATION COMPLETED")
print("===================================")
print("Updated :", updated)
print("Errors  :", errors)
print("Saved   :", OUTPUT_FILE)