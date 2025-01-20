import re
from typing import Optional

import pandas as pd
from litestar.datastructures import State

from kodo.service.controller import build_registry


def flow_welcome_url(node: str, flow: str) -> str:
    node = node[:-1] if node.endswith("/") else node
    flow = flow[1:] if flow.startswith("/") else flow
    return f"{node}/flows/{flow}"


def build_df(state: State) -> pd.DataFrame:
    records = []
    for node in build_registry(state):
        data = node.model_dump()
        host = data.pop("url")
        for flow in data["flows"]:
            flow["url"] = flow_welcome_url(host, flow["url"])
            records.append({**data, **flow})
    COLUMNS = ["name", "description", "organization", "author", "tags",
               "created", "modified", "heartbeat", "url"]
    return pd.DataFrame(records, columns=COLUMNS)


def sort_df(df: pd.DataFrame, by: Optional[str] = None) -> pd.DataFrame:
    DEFAULT_SORT = ["name", "url"]
    if by:
        sort_by = [s.strip().lower() for s in by.split(",")]
        sort_values = [s.split(":")[0] for s in sort_by]
        sort_order = [
            "ascending".startswith(s.split(":")[-1].lower())
            if ":" in s else True for s in sort_by
        ]
    else:
        sort_values = DEFAULT_SORT
        sort_order = [True] * len(DEFAULT_SORT)
        sort_by = DEFAULT_SORT
    if "tags" in sort_values:
        # special handling of list values
        idx = sort_values.index("tags")
        order = sort_order[idx]
        df["tags"] = df.tags.apply(lambda r: sorted(r, reverse=not order))
        df["_tags"] = df.tags.apply(lambda r: "+".join(r))
        sort_values.remove("tags")
        sort_values.insert(idx, "_tags")
    try:
        df.sort_values(by=sort_values, ascending=sort_order, inplace=True)
    except Exception as e:
        sort_by = [f"{e.__class__.__name__}: {e}"]
        df.sort_values(by=DEFAULT_SORT, inplace=True)
    if "_tags" in sort_values:
        df.drop(columns="_tags", inplace=True)
    return df, sort_by


def filter_df(df: pd.DataFrame, q: Optional[str] = None) -> pd.DataFrame:
    if q:
        fdf = df.copy().fillna("")

        def tag(t):
            return fdf.index.isin(
                fdf.explode("tags")[(fdf.explode("tags").tags == t)].index)

        try:
            q = re.sub(
                r'(\w+)\s*~\s*(\".+?\")',
                '(\\1.str.lower().str.contains(\\2.lower()) == True)',
                q)
            fdf = fdf.query(q)
            query = q
        except Exception as e:
            query = f"{e.__class__.__name__}: {e}, fulltext: {q}"
            fdf["_fulltext_"] = fdf.apply(
                lambda r: " ".join([
                    r["organization"],
                    r["name"],
                    r["author"],
                    r["description"],
                    " ".join(r["tags"])
                ]), axis=1)
            fdf = fdf[fdf._fulltext_.str.lower().str.contains(q.lower())]
            fdf = fdf.drop(columns="_fulltext_")
        df = fdf
    else:
        query = None
    return df, query
