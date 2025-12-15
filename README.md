# pivot-episodes
Convert the variable parts of VSKT or VVL datasets from wide to long format: i.e., from rows with episodes (state X from ... to ...) per id into timelines, indexed by id and (year, month), spanning from first pension relevant observation to end of year under investigation. 

For each id and (year, month) various states enter/compete to fill multiple STATUS variables. Default number is 5, with specific fill rules (see attached document), but code may be adjusted to generate fewer or more status and/or change the rules. 


