"""Ask the Genie Agent the demo questions and write genie/transcript.md.

Usage: python scripts/genie_transcript.py --profile <profile> [--target dev]
"""
import argparse
import json
import pathlib
import re
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient

ROOT = pathlib.Path(__file__).resolve().parents[1]
QUESTIONS = [
    ("CFO", "Are we on track for this winter's volume, and where's our biggest risk?"),
    ("FP&A analyst", "Show Eastern Plains' daily volume against expected since November 1, split into weather, customer growth, and other."),
    ("Trading desk", "Do we have enough firm transportation for a design day, and where are we short?"),
    ("CIO", "Does this method work on real Colorado data?"),
    ("Trading desk", "What does the next 7 days of weather mean for volume, by region?"),
]

ap = argparse.ArgumentParser()
ap.add_argument("--profile", required=True)
ap.add_argument("--target", default="dev")
args = ap.parse_args()
ov = json.loads((ROOT / ".databricks" / "bundle" / args.target / "variable-overrides.json").read_text())
catalog, space = ov["catalog"], ov["genie_space_id"]
w = WorkspaceClient(profile=args.profile)

out = ["# Genie Agent transcript", "",
       f"Agent: *Colorado Gas Utility - Winter Volume Plan*. Recorded {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC "
       "with `scripts/genie_transcript.py` through the Genie Conversation API. Each question starts a new conversation.", ""]
for persona, q in QUESTIONS:
    msg = w.genie.start_conversation_and_wait(space, q)
    out += [f"## {persona}: {q}", ""]
    for att in msg.attachments or []:
        if att.text and att.text.content:
            out += ["**Answer**", "", att.text.content.strip(), ""]
        if att.query:
            if att.query.description:
                out += [f"*{att.query.description.strip()}*", ""]
            out += ["```sql", att.query.query.strip(), "```", ""]
            res = w.genie.get_message_attachment_query_result(space, msg.conversation_id, msg.message_id, att.attachment_id)
            sr = res.statement_response
            cols = [c.name for c in sr.manifest.schema.columns]
            rows = (sr.result.data_array or [])[:20]
            out += ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
            out += ["| " + " | ".join("" if v is None else str(v) for v in r) + " |" for r in rows]
            if sr.manifest.total_row_count and sr.manifest.total_row_count > 20:
                out += ["", f"({sr.manifest.total_row_count} rows; first 20 shown)"]
            out += [""]
    print(f"answered: {q[:60]}")

text = "\n".join(out).replace(catalog, "<catalog>")
text = re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[a-z]{2,}", "<user>", text)
(ROOT / "genie" / "transcript.md").write_text(text + "\n")
print("wrote genie/transcript.md")
