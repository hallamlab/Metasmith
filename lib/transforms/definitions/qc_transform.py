from metasmith.models.libraries import DataTypeLibrary, TransformInstanceLibrary

transforms = TransformInstanceLibrary("../qc")
dtypes = DataTypeLibrary.Load("../../data_types/qc.yml")

_ = transforms.AddTypeLibrary("qc", dtypes)
_ = transforms.AddStub("fastqc")
_ = transforms.AddStub("longqc")
transforms.Save()
