from typing import List, Literal, Optional, Union, Tuple, Dict
import asyncio
import ray
from litestar import MediaType, Request, Response, get, post
from litestar.datastructures import State
from litestar.enums import RequestEncodingType
from litestar.exceptions import HTTPException, NotFoundException
from litestar.response import Redirect, Template
from litestar.status_codes import (HTTP_200_OK, HTTP_201_CREATED,
                                   HTTP_400_BAD_REQUEST)
from subprocess import Popen, PIPE
import sys
from asyncio.subprocess import create_subprocess_exec
import kodo.helper as helper
import kodo.service.controller
from kodo.datatypes import MODE, NodeInfo, LaunchResult, Flow
from kodo.log import logger
from kodo.service.flow import build_df, filter_df, flow_welcome_url, sort_df
from kodo.remote.launch import Launcher
import kodo.remote.launcher

# from kodo.worker.process.launcher import FlowLauncher
# from kodo.worker.process.executor import FlowExecutor


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

    # async def _interprocess(
    #         self, 
    #         state: State, 
    #         path: str, 
    #         data:Optional[dict]=None) -> Union[Template, dict]:
    #     url = helper.clean_url(path)
    #     if url not in state.flows:
    #         raise NotFoundException(url)
    #     flow = state.flows[url]

    #     # proc = Popen([sys.executable, "-m", "kodo.tester"], stdout=PIPE, stderr=PIPE)
    #     # out = []
    #     # logger.info("here I am")
    #     # t0 = helper.now()
    #     # while True:
    #     #     try:
    #     #         ret = proc.wait(timeout=0.1)
    #     #         break
    #     #     except:
    #     #         pass
    #     #     if (helper.now() - t0).total_seconds() > 3:
    #     #         proc.kill()
    #     #         logger.error("killing")
    #     #         break
    #     #     await asyncio.sleep(0.1)
    #     # logger.info("done")

 
    #     launch = Launcher(flow.entry)
    #     ret = await launch.enter(data)
    #     # if ret.success:
    #     #     if ret.fid:
    #     #         pass
    #     # print("OK")
    #     return {"I": "am good", "result": ret} 
    
    #     # action = FlowLauncher(flow.entry)
    #     # action_result = await action.enter(data)
    #     # t1 = helper.now() - t0
    #     # meth = logger.info if action_result.returncode == 0 else logger.error
    #     # meth(f"booting `{flow.name}` ({flow.entry}) in {t1}: "
    #     #      f"{'succeeded' if action_result.returncode == 0 else 'failed'}")
    #     # if action_result.returncode == 0:
    #     #     if action_result.fid:
    #     #         t0 = helper.now()
    #     #         executor = FlowExecutor(flow.entry, action_result.fid)
    #     #         proc = await executor.enter(flow)
    #     #         template_file = "launch.html"
    #     #         status = HTTP_201_CREATED
    #     #         t1 = helper.now() - t0
    #     #         logger.info(
    #     #             f"starting `{flow.name}` ({flow.entry}) in {t1}: "
    #     #             f"fid: {action_result.fid}, pid: {proc.pid}")
    #     #     else:
    #     #         status = HTTP_200_OK
    #     #         template_file = "enter.html"
    #     # else:
    #     #     status = HTTP_400_BAD_REQUEST
    #     #     template_file = "error.html"
    #     node = NodeInfo(url=state.url, organization=state.organization)
    #     return Template(
    #         template_name=template_file,
    #         context={
    #             "result": action_result,
    #             "flow": flow,
    #             "node": node,
    #             "url": flow_welcome_url(node.url, flow.url)
    #         },
    #         status_code=status)


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
        logger.info(f"POST /flows/{path}")
        flow, result = await self._handle(state, request, path)
        node = NodeInfo(url=state.url, organization=state.organization)
        ret = {
            "result": result.model_dump(),
            "flow": flow,
            "node": node
        }
        return ret
    
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
