from typing import Dict, Literal, Optional, Tuple, Union

from litestar import MediaType, Request, Response, get, post, route
from litestar.datastructures import State
from litestar.enums import RequestEncodingType
from litestar.exceptions import HTTPException, NotFoundException
from litestar.response import Redirect, Template
from litestar.status_codes import HTTP_200_OK, HTTP_400_BAD_REQUEST

import kodo.helper as helper
import kodo.remote.launcher
import kodo.service.controller
from kodo.datatypes import Flow, LaunchResult, NodeInfo
from kodo.log import logger
from kodo.service.flow import build_df, filter_df, sort_df


class FlowControl(kodo.service.controller.Controller):

    async def _load_data(self, state, p, pp, q, by):
        if pp <= 0 or p < 0:
            logger.warning(f"Invalid pagination params: pp={pp}, p={p}")
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail="Pagination parameters must be positive integers."
            )
        # Build the DataFrame of all flows
        df = build_df(state)
        total = df.shape[0]
        # Apply filtering
        df, query = filter_df(df, q)
        filtered = df.shape[0]
        # Apply sorting
        df, sort_by = sort_df(df, by)
        df.reset_index(drop=True, inplace=True)
        # Apply pagination
        df = df.iloc[p * pp: (p + 1) * pp]
        logger.debug(
            f"return /flows with page {p}/{int(total/pp)} "
            f"and {df.shape[0]}/{total} records")
        return df, total, filtered, sort_by, query

    @route("/flows", http_method=["GET", "POST"],
         summary="Retrieve Active Flows",
         description=(
             "Returns all active flows from nodes and providers in the Kodosumi registry. "
             "Supports filtering, sorting, and pagination.\n\n"
             "**Query Parameters:**\n"
             "- `q` (str, optional): Filter flows by a search query.\n"
             "- `by` (str, optional): Field to sort flows by.\n"
             "- `pp` (int, default=10): Items per page for pagination.\n"
             "- `p` (int, default=0): Page number for pagination.\n"
             "- `format` (json or html, optional): Response format (JSON or HTML)."
         ),
         status_code=HTTP_200_OK,
         tags=["Monitoring", "Flows"])
    async def flows(
            self,
            state: State,
            request: Request,
            format: Optional[Literal["json", "html"]] = None) -> Union[
                Response, Template]:
        if request.method == "POST":
            form = await request.form()
            q = form.get("q", None)
            by = form.get("by", None)
            pp = int(form.get("pp", 20))
            p = int(form.get("p", 0))
        else:
            q = request.query_params.get("q", None)
            by = request.query_params.get("by", None)
            pp = int(request.query_params.get("pp", 20))
            p = int(request.query_params.get("p", 0))
        """
        Return all flows from the nodes and providers masquerading the
        sourcing registry. Returns a pandas DataFrame.
        """
        df, total, filtered, sort_by, query = await self._load_data(
            state, p, pp, q, by)
        if request.method == "POST" or helper.wants_html(request):
            return Template(
                template_name="flows.html",
                context={
                    "items": df.to_dict("records"),
                    "total": total,
                    "filtered": filtered,
                    "p": p,
                    "pp": pp,
                    "q": q or ""},
                status_code=HTTP_200_OK)
        return Response(content={
            "total": total,
            "filtered": filtered,
            "p": p,
            "pp": pp,
            "items": df.to_dict('records'),
            "by": ", ".join(sort_by),
            "q": query
        })


    async def _handle(
            self, 
            state: State, 
            request: Request, 
            path: str) -> Tuple[Flow, LaunchResult]:
        if request.headers.get("content-type") == RequestEncodingType.JSON:
            data = await request.json()
        else:
            data = await request.form()
            data = dict(data)
        url = helper.clean_url(path)
        if url not in state.flows:
            raise NotFoundException(url)
        flow = state.flows[url]
        result = await kodo.remote.launcher.launch(state, flow, data)
        return flow, result

    @get("/flows/{path:path}")
    async def enter_flow(
            self,
            state: State,
            request: Request,
            path: str,
            format: Optional[str]="html") -> Union[Template, Dict, Redirect]:
        logger.info(f"GET /flows{path}")
        flow, result = await self._handle(state, request, path)
        node = NodeInfo(url=state.url, organization=state.organization)
        ret = {
            "result": result,
            "flow": flow,
            "node": node
        }
        if helper.wants_html(request):
            if result.is_launch:
                return Redirect(f"/flow/{result.fid}")
            return Template(
                template_name="enter.html", 
                context={**ret, "width": "narrow"}, 
                media_type=MediaType.HTML)
        return ret

    @post("/flows/{path:path}")
    async def launch_flow(
            self,
            state: State,
            request: Request,
            path: str) -> Union[dict, Redirect, Template]:
        logger.info(f"POST /flows{path}")
        flow, result = await self._handle(state, request, path)
        node = NodeInfo(url=state.url, organization=state.organization)
        ret = {
            "result": result.model_dump(),
            "flow": flow,
            "node": node
        }
        if result.is_launch:
            return Redirect(f"/flow/{result.fid}")
        return Template(
            template_name="enter.html", 
            context=ret, 
            media_type=MediaType.HTML)

    
    @get("/flows/counts",
         summary="Retrieve Node and Flow Counts",
         description="Provides total counts of nodes and flows grouped by organization and tags in the Kodosumi registry.",
         tags=["Monitoring", "Statistics"])
    async def counts(
            self,
            state: State,
            request: Request,
            format: Optional[Literal["json", "html"]] = None) -> Response:
        """
        Retrieves counts of nodes and flows grouped by organization and tags.
        Supports optional response format (JSON or HTML).
        """
        # Validate the format parameter
        if format not in (None, "json", "html"):
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail="Invalid format. Allowed values are 'json' or 'html'."
            )
        # Build the DataFrame of all flows
        df = build_df(state)
        total_count = df.shape[0]

        # Group by organization and tags for counts
        organization_counts = df.groupby(
            "organization").name.nunique().to_dict()
        tag_counts = df.explode("tags").groupby(
            "tags").name.nunique().to_dict()

        logger.debug(
            f"Retrieved counts: {total_count} total, "
            f"{len(organization_counts)} organizations, {
                len(tag_counts)} tags."
        )
        # Prepare the response content
        content = {
            "total": total_count,
            "organization": organization_counts,
            "tags": tag_counts
        }
        return Response(content=content)
