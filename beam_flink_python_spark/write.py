
file  = open('/home/israel/Documents/beam/modulos/input/37723973622863_20210617_A_RAIABD-TB_CANAL_VENDA.json',)






conteudo = file.readline()
tamanho = len(conteudo)
#print(tamanho)
#,mode='r',encoding='UTF-8'
#conteudo.seek(0)
#conteudo.flush()


#linhas = '{"schema":{"type":"struct","fields":[{"type":"string","optional":true,"field":"table"},{"type":"string","optional":true,"field":"op_type"},{"type":"string","optional":true,"field":"op_ts"},{"type":"string","optional":true,"field":"current_ts"},{"type":"string","optional":true,"field":"pos"},{"type":"array","items":{"type":"string","optional":false},"optional":true,"field":"primary_keys"},{"type":"map","keys":{"type":"string","optional":false},"values":{"type":"string","optional":false},"optional":true,"field":"tokens"},{"type":"struct","fields":[{"type":"int64","optional":true,"field":"CD_CANAL_VENDA"},{"type":"string","optional":true,"field":"DS_CANAL_VENDA"}],"optional":true,"name":"row","field":"before"},{"type":"struct","fields":[{"type":"int64","optional":true,"field":"CD_CANAL_VENDA"},{"type":"string","optional":true,"field":"DS_CANAL_VENDA"}],"optional":true,"name":"row","field":"after"}],"optional":false,"name":"A_RAIABD.TB_CANAL_VENDA"},"payload":{"table":"A_RAIABD.TB_CANAL_VENDA","op_type":"U","op_ts":"2021-06-17 17:31:31.689976","current_ts":"2021-06-17 17:31:37.785001","pos":"00000000050224513002","primary_keys":["CD_CANAL_VENDA","DS_CANAL_VENDA"],"tokens":{"txid":"0.442.2.1856248"},"before":{"CD_CANAL_VENDA":2,"DS_CANAL_VENDA":"Televendas"},"after":{"CD_CANAL_VENDA":2,"DS_CANAL_VENDA":"Televendas"}}}]'


#cont = 0

#while (cont < 10000000):
#    file.write(linhas)
#    cont = cont + 1 # ou cont += 1

11000000

#for linha in conteudo:
for l in linha:
    l.write(linhas)
print(conteudo)
print(tamanho)





#linhas = '{"schema":{"type":"struct","fields":[{"type":"string","optional":true,"field":"table"},{"type":"string","optional":true,"field":"op_type"},{"type":"string","optional":true,"field":"op_ts"},{"type":"string","optional":true,"field":"current_ts"},{"type":"string","optional":true,"field":"pos"},{"type":"array","items":{"type":"string","optional":false},"optional":true,"field":"primary_keys"},{"type":"map","keys":{"type":"string","optional":false},"values":{"type":"string","optional":false},"optional":true,"field":"tokens"},{"type":"struct","fields":[{"type":"int64","optional":true,"field":"CD_CANAL_VENDA"},{"type":"string","optional":true,"field":"DS_CANAL_VENDA"}],"optional":true,"name":"row","field":"before"},{"type":"struct","fields":[{"type":"int64","optional":true,"field":"CD_CANAL_VENDA"},{"type":"string","optional":true,"field":"DS_CANAL_VENDA"}],"optional":true,"name":"row","field":"after"}],"optional":false,"name":"A_RAIABD.TB_CANAL_VENDA"},"payload":{"table":"A_RAIABD.TB_CANAL_VENDA","op_type":"U","op_ts":"2021-06-17 17:31:31.689976","current_ts":"2021-06-17 17:31:37.785001","pos":"00000000050224513002","primary_keys":["CD_CANAL_VENDA","DS_CANAL_VENDA"],"tokens":{"txid":"0.442.2.1856248"},"before":{"CD_CANAL_VENDA":2,"DS_CANAL_VENDA":"Televendas"},"after":{"CD_CANAL_VENDA":2,"DS_CANAL_VENDA":"Televendas"}}}]'


cont = 0

while (cont < 1):
     file.write(linhas)
     cont = cont += 1



        for linha in arquivo:
            print(linha)

   #| 'line text'    >> beam.Create(['149633CM,Marco,10,Accounts,1-01-2019'])
   #| 'read bucket'  >> beam.io.ReadFromText('/home/israel/Documents/flink-code/input/Televenda.csv')