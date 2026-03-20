from abc import ABC, abstractmethod


class BaseGraphGenerator(ABC):
    def __init__(self) -> None:
        self.bind_db_to_repos()

    @abstractmethod
    def bind_db_to_repos(self) -> None:
        pass

    @abstractmethod
    def generate_graph_data(self) -> None:
        pass