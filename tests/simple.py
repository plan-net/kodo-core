from crewai import Agent, Task, Crew, Process
from crewai_tools import tool
from langchain_openai import ChatOpenAI
import datetime

from kodo.common import publish, Launch


llm = ChatOpenAI(model="ollama/phi3:3.8b", base_url="http://localhost:11434")
# llm = ChatOpenAI()


# Tools
@tool("Mock Tool")
def mock_tool(query: str) -> str:
    """Evaluate query and say 'I like it' or say 'I don't like it'."""
    return "I like it"


# Agents
story_architect = Agent(
    name="Hymn Architect",
    role="Hymn Planner",
    goal="Create a topic outline for a short hymn.",
    backstory="An experienced hymn author with a knack for engaging plots.",
    llm=llm,
    tools=[mock_tool],
    max_iter=3,
    verbose=True
)

narrative_writer = Agent(
    name="Hymn Writer",
    role="Hymn Writer",
    goal="Write a short hymn based on the outline with no more than 150 words.",
    backstory="A creative hymn writer who brings stories to life with vivid descriptions.",
    llm=llm,
    max_iter=3,
    verbose=True
)

# Tasks
task_outline = Task(
    name="Hymn Outline Creation",
    agent=story_architect,
    description='Generate a structured plot outline for a short hymn about "{topic}".',
    expected_output="A detailed plot outline with key tension arc."
)

task_story = Task(
    name="Story Writing",
    agent=narrative_writer,
    description="Write the full hymn using the outline details and tension arc.",
    context=[task_outline],
    expected_output="A complete short hymn about {topic} with a beginning, middle, and end."
)

# Crew
crew = Crew(
    agents=[
        story_architect, 
        narrative_writer,
    ],
    tasks=[
        task_outline, 
        task_story,
    ],
    process=Process.sequential,
    verbose=True
)

flow = publish(
    crew, 
    url="/test/hymn1",
    name="Hymn Creator",
    description="Create a short hymn based on a given topic."
)

@flow.enter
def landing_page(form):
    if topic:=form.get("topic"):
        if topic.strip():
            return Launch(topic=topic.strip())
    return """
        <h2>
            hello world
        </h2>
        <hr/>
        <input type="text" name="topic" placeholder="Enter a topic">
        <input type="submit" value="GO FOR IT">
    """


if __name__ == "__main__":
    t0 = datetime.datetime.now()
    result = crew.kickoff(inputs={
        #"topic": "The truth always settles",
        "topic": "Lies don't travel far."
    })
    print("Final Story:", result)
    print(datetime.datetime.now() - t0)
