from SparkBD import SparkBD

'''
MLS,Location,Price,Bedrooms,Bathrooms,Size,Price SQ Ft,Status
132842,Arroyo Grande,795000.00,3,3,2371,335.30,Short Sale
134364,Paso Robles,399000.00,4,3,2818,141.59,Short Sale
'''


def monta_tupla(dados: str):
    vals = dados.split(',')
    return int(vals[3]), (float(vals[2]), 1)


original = SparkBD("real_state")

original.get_rdd("RealEstate.csv")

original.filter(lambda x: x.split(',')[0] != 'MLS')

original.map(monta_tupla)  # quartos,preco,qnt_casa

original.reduce_by_key(lambda casa1, casa2: (casa1[0] + casa2[0], casa1[1] + casa2[1]))

original.map_values(lambda t: t[0] / t[1])

original.sort_by(lambda x: x[0], ascending=True)

original.save_rdd_to_file('real_state', coalesce=1)

# print(original)
