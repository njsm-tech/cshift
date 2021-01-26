class Comparison:
    @classmethod
    def datasets_same(cls, *datasets):
        """
        Checks for distributional shift between datasets using the 
            comparison specified by the subclass. 
        Returns True if datasets are the same (no shift) and 
            False if datasets do not represent the same distribution 
            (observed covariate shift). 

        Implemented by subclasses.
        """
        raise NotImplementedError()
