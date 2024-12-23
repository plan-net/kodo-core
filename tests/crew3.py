from crewai import Agent, Task, Crew, Process
from langchain_openai import ChatOpenAI
import datetime

llm = ChatOpenAI(model="ollama/phi3:3.8b", base_url="http://localhost:11434")

# Agents
story = Agent(
    name="Joker",
    role="Joke Teller",
    goal="Tell me a short joke.",
    backstory="A funny agent which behaves unpredictable.",
    llm=llm,
    max_iter=3,
    verbose=True
)
# Tasks
joke = Task(
    name="Joke Creation",
    agent=story,
    description='Invent a new joke.',
    expected_output="The joke."
)
# Crew
crew = Crew(
    agents=[story],
    tasks=[joke],
    process=Process.sequential,
    verbose=True
)

from kodo.worker.loader import publish
flow = publish(crew, url="/hymn1", name="Hymn Creator")

@flow.welcome
async def landing_page():
    return "hello world"


if __name__ == "__main__":
    t0 = datetime.datetime.now()
    result = crew.kickoff(inputs={})
    print("Final Story:", result)
    print(datetime.datetime.now() - t0)
