import os
import csv
from enum import Enum
from functools import lru_cache

from langchain.pydantic_v1 import BaseModel, Field
from langchain.tools.retriever import create_retriever_tool
from langchain_community.agent_toolkits.connery import ConneryToolkit
from langchain_community.retrievers import (
    KayAiRetriever,
    PubMedRetriever,
    WikipediaRetriever,
)
from langchain_community.retrievers.you import YouRetriever
from langchain_community.tools import ArxivQueryRun, DuckDuckGoSearchRun
from langchain_community.tools.connery import ConneryService
from langchain_community.tools.tavily_search import TavilyAnswer, TavilySearchResults
from langchain_community.utilities.arxiv import ArxivAPIWrapper
from langchain_community.utilities.tavily_search import TavilySearchAPIWrapper
from langchain_community.vectorstores.redis import RedisFilter
from langchain_robocorp import ActionServerToolkit
from langchain.tools import tool, BaseTool
from typing import Optional, Type
from langchain.callbacks.manager import (
    AsyncCallbackManagerForToolRun,
    CallbackManagerForToolRun,
)

import boto3
from botocore.exceptions import NoCredentialsError

from app.upload import vstore


class DDGInput(BaseModel):
    query: str = Field(description="search query to look up")


class ArxivInput(BaseModel):
    query: str = Field(description="search query to look up")


class PythonREPLInput(BaseModel):
    query: str = Field(description="python command to run")


RETRIEVAL_DESCRIPTION = """Can be used to look up information that was uploaded to this assistant.
If the user is referencing particular files, that is often a good hint that information may be here.
If the user asks a vague question, they are likely meaning to look up info from this retriever, and you should call it!"""


def get_retriever(assistant_id: str):
    return vstore.as_retriever(
        search_kwargs={"filter": RedisFilter.tag("namespace") == assistant_id}
    )


@lru_cache(maxsize=5)
def get_retrieval_tool(assistant_id: str, description: str):
    return create_retriever_tool(
        get_retriever(assistant_id),
        "Retriever",
        description,
    )

class CSVInput(BaseModel):
    header: list[str] = Field(description="The header of the CSV file")
    data: list[list[str]] = Field(description="The data of the CSV file")
    file_name: str = Field(description="The file name of the CSV file")

def create_csv(header, data, file_name):
    folder_path = "data"
    csv_file = os.path.join(folder_path, file_name)
    # TODEL
    csv_output = csv_file + ".csv"
    #TODEL
    
    # Ensure the folder exists
    os.makedirs(folder_path, exist_ok=True)
    
    # Writing to the CSV file
    with open(csv_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        # Writing the header
        writer.writerow(header)
        
        # Writing the data rows
        for row in data:
            writer.writerow(row)

    return csv_file

def upload_file_to_wasabi(file_name):
    """
    Upload a file to Wasabi Hot Cloud Storage and make it publicly accessible.

    :param file_name: File to upload
    :return: Public URL of the uploaded file
    """

    wasabi_region = 'ap-southeast-1'
    bucket_name = 'tesa-medication-schedules'
    object_name = None

    if object_name is None:
        object_name = file_name.split('/')[-1]

    s3_client = boto3.client('s3', 
                             endpoint_url=f'https://s3.{wasabi_region}.wasabisys.com',
                             aws_access_key_id=os.environ['WASABI_ACCESS_KEY'],
                             aws_secret_access_key=os.environ['WASABI_SECRET_KEY'],
                             region_name=wasabi_region)  # Endpoint and auth for Wasabi
    
    file_path = os.path.join('data', file_name)

    try:
        s3_client.upload_file(file_path, bucket_name, object_name, 
                              ExtraArgs={'ACL': 'public-read'})  # Make file public
        url = f"https://{bucket_name}.s3.{wasabi_region}.wasabisys.com/{object_name}"
        return url
    except NoCredentialsError:
        print("Credentials not available")
        return None

def upload_file_to_s3(file_name):
    """
    Upload a file to Amazon S3 and make it publicly accessible.

    :param file_name: File to upload
    :return: Public URL of the uploaded file
    """

    s3_region = 'ap-southeast-1'  # S3 region
    bucket_name = 'tesa-medication-schedules';  # Replace with your bucket name
    object_name = None

    if object_name is None:
        object_name = os.path.basename(file_name)

    s3_client = boto3.client('s3', 
                             aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                             aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                             region_name=s3_region)  # Endpoint and auth for S3
    
    file_path = os.path.join('data', file_name)

    try:
        s3_client.upload_file(file_path, bucket_name, object_name, 
                              ExtraArgs={'ACL': 'public-read'})  # Make file public
        url = f"https://{bucket_name}.s3.{s3_region}.amazonaws.com/{object_name}"
        return url
    except Exception as e:
        print(f"Error uploading file to S3: {e}")
        return None


def get_all_files_in_data_folder(folder_path="data"):
    files = os.listdir(folder_path)
    return files

class CreateCSV(BaseTool):
    name = "create_csv"
    description = "Create a CSV file with the given header and data."
    args_schema: Type[BaseModel] = CSVInput

    def _run(
        self, header: list[str], data: list[list[str]], file_name: str, run_manager: Optional[CallbackManagerForToolRun] = None
    ) -> str:
        """Use the tool."""
        return create_csv(header, data, file_name)

    async def _arun(
        self, header: list[str], data: list[list[str]], file_name: str, run_manager: Optional[AsyncCallbackManagerForToolRun] = None
    ) -> str:
        """Use the tool asynchronously."""
        return create_csv(header, data, file_name)

class UploadCSVInput(BaseModel):
    file_name: str = Field(description="The file name of the CSV file")

class UploadCSV(BaseTool):
    name = "upload_csv"
    description = "Upload a CSV file to Wasabi Hot Cloud Storage."
    args_schema: Type[BaseModel] = UploadCSVInput

    def _run(
        self, file_name: str, run_manager: Optional[CallbackManagerForToolRun] = None
    ) -> str:
        """Use the tool."""
        return upload_file_to_s3(file_name)

    async def _arun(
        self, file_name: str, run_manager: Optional[AsyncCallbackManagerForToolRun] = None
    ) -> str:
        """Use the tool asynchronously."""
        return upload_file_to_s3(file_name)

class GetAllFilesInput(BaseModel):
    folder_path: str = Field(description="The folder path to get the files from")

class GetFilesInDataFolder(BaseTool):
    name = "get_files_medication_schedules_in_data_folder"
    description = "Get all files / generated medication schedules in the data folder."
    args_schema: Type[BaseModel] = GetAllFilesInput

    def _run(
        self, folder_path: str, run_manager: Optional[CallbackManagerForToolRun] = None
    ) -> list[str]:
        """Use the tool."""
        return get_all_files_in_data_folder(folder_path="data")

    async def _arun(
        self, folder_path: str, run_manager: Optional[AsyncCallbackManagerForToolRun] = None
    ) -> list[str]:
        """Use the tool asynchronously."""
        return get_all_files_in_data_folder(folder_path="data")

# @lru_cache(maxsize=1)
# def _get_duck_duck_go():
#     return DuckDuckGoSearchRun(args_schema=DDGInput)


# @lru_cache(maxsize=1)
# def _get_arxiv():
#     return ArxivQueryRun(api_wrapper=ArxivAPIWrapper(), args_schema=ArxivInput)


# @lru_cache(maxsize=1)
# def _get_you_search():
#     return create_retriever_tool(
#         YouRetriever(n_hits=3, n_snippets_per_hit=3),
#         "you_search",
#         "Searches for documents using You.com",
#     )


# @lru_cache(maxsize=1)
# def _get_sec_filings():
#     return create_retriever_tool(
#         KayAiRetriever.create(
#             dataset_id="company", data_types=["10-K", "10-Q"], num_contexts=3
#         ),
#         "sec_filings_search",
#         "Search for a query among SEC Filings",
#     )


# @lru_cache(maxsize=1)
# def _get_press_releases():
#     return create_retriever_tool(
#         KayAiRetriever.create(
#             dataset_id="company", data_types=["PressRelease"], num_contexts=6
#         ),
#         "press_release_search",
#         "Search for a query among press releases from US companies",
#     )


# @lru_cache(maxsize=1)
# def _get_pubmed():
#     return create_retriever_tool(
#         PubMedRetriever(), "pub_med_search", "Search for a query on PubMed"
#     )


# @lru_cache(maxsize=1)
# def _get_wikipedia():
#     return create_retriever_tool(
#         WikipediaRetriever(), "wikipedia", "Search for a query on Wikipedia"
#     )


# @lru_cache(maxsize=1)
# def _get_tavily():
#     tavily_search = TavilySearchAPIWrapper()
#     return TavilySearchResults(api_wrapper=tavily_search)


# @lru_cache(maxsize=1)
# def _get_tavily_answer():
#     tavily_search = TavilySearchAPIWrapper()
#     return TavilyAnswer(api_wrapper=tavily_search)


# @lru_cache(maxsize=1)
# def _get_action_server():
#     toolkit = ActionServerToolkit(
#         url=os.environ.get("ROBOCORP_ACTION_SERVER_URL"),
#         api_key=os.environ.get("ROBOCORP_ACTION_SERVER_KEY"),
#     )
#     tools = toolkit.get_tools()
#     return tools


@lru_cache(maxsize=1)
def _get_connery_actions():
    connery_service = ConneryService(
        runner_url=os.environ.get("CONNERY_RUNNER_URL"),
        api_key=os.environ.get("CONNERY_RUNNER_API_KEY"),
    )
    connery_toolkit = ConneryToolkit.create_instance(connery_service)
    tools = connery_toolkit.get_tools()
    return tools


class AvailableTools(str, Enum):
    # ACTION_SERVER = "Action Server by Robocorp"
    CONNERY = '"AI Action Runner" by Connery'
    # DDG_SEARCH = "DDG Search"
    # TAVILY = "Search (Tavily)"
    # TAVILY_ANSWER = "Search (short answer, Tavily)"
    RETRIEVAL = "Retrieval"
    # ARXIV = "Arxiv"
    # YOU_SEARCH = "You.com Search"
    # SEC_FILINGS = "SEC Filings (Kay.ai)"
    # PRESS_RELEASES = "Press Releases (Kay.ai)"
    # PUBMED = "PubMed"
    # WIKIPEDIA = "Wikipedia"
    CREATE_CSV = "Create CSV"
    UPLOAD_CSV = "Upload CSV"
    GET_ALL_FILES = "Get all files in data folder"


TOOLS = {
    # AvailableTools.ACTION_SERVER: _get_action_server,
    AvailableTools.CONNERY: _get_connery_actions,
    # AvailableTools.DDG_SEARCH: _get_duck_duck_go,
    # AvailableTools.ARXIV: _get_arxiv,
    # AvailableTools.YOU_SEARCH: _get_you_search,
    # AvailableTools.SEC_FILINGS: _get_sec_filings,
    # AvailableTools.PRESS_RELEASES: _get_press_releases,
    # AvailableTools.PUBMED: _get_pubmed,
    # AvailableTools.TAVILY: _get_tavily,
    # AvailableTools.WIKIPEDIA: _get_wikipedia,
    # AvailableTools.TAVILY_ANSWER: _get_tavily_answer,
    AvailableTools.CREATE_CSV: CreateCSV,
    AvailableTools.UPLOAD_CSV: UploadCSV,
    AvailableTools.GET_ALL_FILES: get_all_files_in_data_folder,
}

TOOL_OPTIONS = {e.value: e.value for e in AvailableTools}

# Check if dependencies and env vars for each tool are available
for k, v in TOOLS.items():
    # Connery requires env vars to be valid even if the tool isn't used,
    # so we'll skip the check for it
    if k != AvailableTools.CONNERY:
        v()
