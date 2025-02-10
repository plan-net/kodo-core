from typing import Dict, List, Literal, Optional, Tuple, Union

from litestar import MediaType, Request, Response, get, post
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
    path = "/flows"

    @get("/",
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
         tags=["Monitoring", "Flows"])
    async def flows(
            self,
            state: State,
            request: Request,
            q: Optional[str] = None,
            by: Optional[str] = None,
            pp: int = 10,
            p: int = 0,
            format: Optional[Literal["json", "html"]] = None) -> Union[
                Response, Template]:
        """
        Return all flows from the nodes and providers masquerading the
        sourcing registry. Returns a pandas DataFrame.
        """
        # Validate pagination parameters
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

        # Handle HTML or JSON response
        if (("text/html" in request.headers.get("accept", "")
             and format != "json") | (format == "html")):
            return Template(
                template_name="explore.html",
                context={
                    "result": df.to_dict("records"),
                    "total": total,
                    "filtered": filtered,
                    "p": p,
                    "pp": pp,
                    "q": q},
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

    @get("/{path:path}")
    async def enter_flow(
            self,
            state: State,
            request: Request,
            path: str,
            format: Optional[str]="html") -> Union[Template, Dict]:
        logger.info(f"GET /flows{path}")
        provided_types: List[str] = [MediaType.JSON, MediaType.HTML]
        preferred_type = request.accept.best_match(
            provided_types, default=MediaType.JSON)
        flow, result = await self._handle(state, request, path)
        node = NodeInfo(url=state.url, organization=state.organization)
        ret = {
            "result": result,
            "flow": flow,
            "node": node
        }
        if preferred_type == MediaType.JSON or format == "json":
            return ret
        return Template(
            template_name="enter.html", 
            context=ret, 
            media_type=MediaType.HTML)

    @post("/{path:path}")
    async def launch_flow(
            self,
            state: State,
            request: Request,
            path: str) -> Union[dict, Redirect]:
        logger.info(f"POST /flows{path}")
        flow, result = await self._handle(state, request, path)
        node = NodeInfo(url=state.url, organization=state.organization)
        ret = {
            "result": result.model_dump(),
            "flow": flow,
            "node": node
        }
        return Redirect(f"/flow/{result.fid}")
        #return ret
    
    @get("/counts",
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
