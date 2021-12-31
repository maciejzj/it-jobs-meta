import pandera as pa

class MySychema(pa.SchemaModel):
    id: pa.String = pa.Field()
    name: pa.String = pa.Field()
    posted: pa.Timestamp = pa.Field()
    title: pa.String = pa.Field()
    technology: pa.String = pa.Field()
    category: pa.String = pa.Field()
    url: pa.String = pa.Field()
    remote: pa.Bool = pa.Field()
