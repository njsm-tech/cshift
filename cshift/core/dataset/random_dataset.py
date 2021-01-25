from .dataset import Dataset

class RandomDataset(Dataset):
    @classmethod
    def generate(cls, size: Tuple[int]):
        nrows = size[0]
        ncols = size[1]
        cols = []
        for i, col in enumerate(cols):
            name = 'col' + str(i)
            cols.append(Column.generate(name=name, size=nrows))
        return cls.from_columns(cols)
