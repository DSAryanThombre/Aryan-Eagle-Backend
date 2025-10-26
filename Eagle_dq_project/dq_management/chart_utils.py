import io
import base64
from typing import Dict, Any, List

import pandas as pd
import seaborn as sns
import matplotlib

# Use non-GUI backend
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def make_criticality_bar_chart(chart_data: Dict[str, Dict[str, int]]) -> str:
    """
    Build a stacked bar chart (Failed vs Passed) per Criticality using seaborn/matplotlib
    and return a data URL (data:image/png;base64,...) suitable for embedding in HTML.

    chart_data expected structure:
    {
        "failed": {"Critical": int, "High": int, "Medium": int, "Low": int},
        "passed": {"Critical": int, "High": int, "Medium": int, "Low": int}
    }
    """
    criticalities: List[str] = ["Critical", "High", "Medium", "Low"]

    # Safe extraction with defaults
    failed = {k: int(chart_data.get("failed", {}).get(k, 0) or 0) for k in criticalities}
    passed = {k: int(chart_data.get("passed", {}).get(k, 0) or 0) for k in criticalities}

    # Prepare DF in long format for seaborn
    rows: List[Dict[str, Any]] = []
    for crit in criticalities:
        rows.append({"Criticality": crit, "Status": "Failed", "Count": failed[crit]})
        rows.append({"Criticality": crit, "Status": "Passed", "Count": passed[crit]})

    df = pd.DataFrame(rows)

    # Plot
    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=(8, 4.5), dpi=150)

    # Create stacked bars by plotting Failed then Passed with bottom
    # Compute bottoms for stacking
    failed_counts = df[df["Status"] == "Failed"].set_index("Criticality")["Count"]
    passed_counts = df[df["Status"] == "Passed"].set_index("Criticality")["Count"]

    x = range(len(criticalities))
    ax.bar(x, [failed_counts.get(c, 0) for c in criticalities], label="Failed", color="#dc2626")
    ax.bar(x, [passed_counts.get(c, 0) for c in criticalities], bottom=[failed_counts.get(c, 0) for c in criticalities], label="Passed", color="#10b981")

    ax.set_xticks(list(x))
    ax.set_xticklabels(criticalities)
    ax.set_ylabel("Test Groups")
    ax.set_title("Test Result Summary by Criticality")
    ax.legend(loc="best")

    plt.tight_layout()

    # Export to PNG in-memory
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)

    encoded = base64.b64encode(buf.read()).decode("ascii")
    return f"data:image/png;base64,{encoded}"
