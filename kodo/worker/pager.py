from litestar.pagination import AbstractSyncClassicPaginator
import kodo.types
import pandas as pd


class FlowPaginator(AbstractSyncClassicPaginator[kodo.types.FlowRecord]):
    def __init__(self, df) -> None:
        self.df = df

    def get_total(self, pp: int) -> int:
        return round(len(self.df) / pp)

    # def get_items(
    #         self, 
    #         p: int, 
    #         pp: int) -> list[kodo.types.FlowRecord]:
    #     return [
    #         self.df.iloc[i : i + pp] 
    #         for i in range(0, len(self.df), pp)
    #     ][p]

    @property
    def df(
            self,
            p: int,
            pp: int): pd.DataFrame:
        return self.df.iloc[p * pp : (p + 1) * pp]
