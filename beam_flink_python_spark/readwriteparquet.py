import grpc
import json
import apache_beam as beam
import pyarrow as pa
import logging
from apache_beam.io import filebasedsink
from apache_beam.io import filebasedsource
from apache_beam.io.aws.s3io import S3IO
from apache_beam.io.aws.clients.s3 import messages
from apache_beam.io.iobase import RangeTracker
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io.iobase import Read
from apache_beam.io.iobase import Write
from apache_beam.transforms import PTransform
from apache_beam.transforms import DoFn
from apache_beam.transforms import ParDo
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.version import __version__ as beam_version
import apache_beam.transforms.window as window
options = PipelineOptions()



def run():
    options = PipelineOptions([
        "--runner=FlinkRunner",
        "--flink_version=1.13.0",
        "--flink_master=localhost:8081",
        "--environment_type=EXTERNAL",
        "--environment_config=localhost:50000"
    ])

with beam.Pipeline(options=options) as p:

       (p       
           | 'Read Json'  >> beam.io.parquetio.ReadFromParquet('s3://raiadrogasil-datalake-dev-us-east-1-109196921142-stage/kafka/stage_db/tb_canal_venda/partition=2021-08-18/part-00000-c122e72f-e591-4623-9fcc-c5daaa5aaa64.c000.snappy.parquet')

           | 'write Parquet' >> beam.io.parquetio.WriteToParquet('/home/israel/Documents/beam/modulos/output/TB_PRODUTO',pa.schema([,(CD_CATEGORIA pa.string()),
           (CD_CATEGORIA_EXPOSICAO pa.string()),(CD_CATEGORIA_INTERNET pa.string()),
           (CD_CATEGORIA_MASTER pa.string()),(CD_CATEGORIA_MASTER_INTERNET pa.string()),
           (CD_CLASSETERAP pa.string()),(CD_CLASSIF_COMERCIAL pa.string()),(CD_CLASSIF_PROD pa.string()),
           (CD_CLAS_FISCAL pa.string()),(CD_CLAS_FISCAL_TIPI pa.string()),(CD_COMERCIALIZACAO pa.string()),
           (CD_COMPOSICAO pa.string()),(CD_DCB pa.string()),(CD_FORMA_FARMA pa.string()),
           (CD_FORNECEDOR_ULT_ENTRADA pa.string()),(CD_GRCOMPRAS pa.string()),(CD_GRFICHA pa.string()),
           (CD_GRUPO pa.string()),(CD_GRUPO_INTERNET pa.string()),(CD_GRUPO_REMARCACAO pa.string()),
           (CD_GRUPO_REMARCACAO_INT pa.string()),(CD_INDICA pa.string()),(CD_LISTA_PIS_COFINS pa.string()),
           (CD_LOCAL_PRODUTO pa.string()),(CD_LOCAL_PRODUTO_LOJA pa.string()),(CD_MENSAGEM pa.string()),
           (CD_OPERADOR pa.string()),(CD_PRINCIPIO_ATIVO_COMPOSTO pa.string()),(CD_PRODUTO pa.string()),
           (CD_PRODUTO_COMPACTO pa.string()),(CD_PRODUTO_LINHA pa.string()),(CD_PRODUTO_MARCA pa.string()),
           (CD_PRODUTO_ORIGEM pa.string()),(CD_PRODUTO_SAZONALIDADE pa.string()),(CD_PRODUTO_SEM_ETIQUETA pa.string()),
           (CD_PRODUTO_STATUS pa.string()),(CD_PRODUTO_SUBMARCA pa.string()),(CD_PRODUTO_VLR_AGREGADO pa.string()),
           (CD_PSICOTROPICOS pa.string()),(CD_SAL pa.string()),(CD_SETOR_SEPARACAO pa.string()),(CD_SUBGRUPO pa.string()),
           (CD_SUBGRUPO_INTERNET pa.string()),(CD_SUB_CATEGORIA pa.string()),(CD_SUB_CATEGORIA_INTERNET pa.string()),
           (CD_TARJA pa.string()),(CD_TIPO_ETQ pa.string()),(CD_UNIDADE pa.string()),(CD_UNIDADE_FORMA_QUANTIDADE pa.string()),
           (CD_USO pa.string()),(CL_CURVA_FIN pa.string()),(CL_CURVA_FIS pa.string()),(DS_PRODUTO pa.string()),
           (DS_PRODUTO_ANVISA pa.string()),(DS_PRODUTO_COR_RGB pa.string()),(DS_PRODUTO_DET pa.string()),(DS_PRODUTO_FONETICA pa.string()),
           (DS_PRODUTO_SIAC pa.string()),(DS_PRODUTO_TECNICA pa.string()),(DT_ATUALIZACAO_PDV pa.string()),(DT_BLOQUEIA_ENCOMENDA pa.string()),
           (DT_CADASTRO pa.string()),(DT_PRIMEIRA_ENTRADA pa.string()),(DT_PRODUTO_INATIVO pa.string()),(DT_REGULARIZACAO_STATUS pa.string()),
           (DT_ULT_ALT_CLAS_FISCAL pa.string()),(DT_ULT_ENTRADA pa.string()),(DT_ULT_REMARC pa.string()),(DT_VENCIMENTO_REGISTRO_ANVISA pa.string()),
           (FL_AVULSO pa.string()),(FL_BLOQUEIA_ENCOMENDA pa.string()),(FL_BMPO pa.string()),(FL_BSPO pa.string()),(FL_CADASTRO_GENERICO pa.string()),
           (FL_CADASTRO_PROVISORIO pa.string()),(FL_CARTELADO_NAO_NEGOCIADO pa.string()),(FL_COMPRA_FABRICANTE pa.string()),(FL_CUPONAVEL pa.string()),
           (FL_DELETADO pa.string()),(FL_DESATIVACAO_PSICO pa.string()),(FL_ENVIO_SNGPC pa.string()),(FL_ESPACO_BELEZA pa.string()),(FL_EXIGE_CUPOM pa.string()),
           (FL_HABILITA_EAN pa.string()),(FL_INATIVO pa.string()),(FL_INTERNET pa.string()),(FL_JATEAVEL pa.string()),(FL_LOTE_FATURAMENTO pa.string()),
           (FL_MARCA_PROPRIA pa.string()),(FL_OTC pa.string()),(FL_PSICO_BALANCO_CONTR_ESP pa.string()),(FL_TRANSMITIDO pa.string()),
           (MD_ALTURA_CM double  pa.string()),(MD_COMPRIMENTO_CM double  pa.string()),(MD_LARGURA_CM double  pa.string()),(NR_REGISTRO_ANVISA pa.string()),
           (NR_SHELF_LIFE pa.string()),(NR_ULT_LOTE pa.string()),(PC_ICMS_ENTRADA pa.string()),(PC_IPI pa.string()),(QT_DOSAGEM double  pa.string()),
           (QT_EMBALAGEM pa.string()),(QT_PESO double  pa.string()),(QT_PRODUTO_COMPACTO pa.string()),(QT_UNID_MAX_ABAST pa.string()),
           (VL_CUSTO_MEDIO double  pa.string()),(VL_CUSTO_MEDIO_ANTERIOR pa.string()),(VL_CUSTO_MEDIO_SEM_TRIB pa.string()),(VL_CUSTO_RAIA double  pa.string()),
           (VL_CUSTO_ULT_ENTRADA pa.string()),(VL_DESCONTO_FINANCEIRO_MEDIO pa.string()),(VL_ICMS_MEDIO_ENTRADA pa.string())],file_name_suffix='.parquet')
       )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()


    schema = pyarrow.schema([('cd_canal_venda', pa.int64()),( 'ds_canal_venda', pa.string()),( 'dt_process_stage', schema = pa.schema([
    (CD_CATEGORIA, pa.string()),
    (CD_CATEGORIA_EXPOSICAO, pa.string()),
    (CD_CATEGORIA_INTERNET, pa.string()),
    (CD_CATEGORIA_MASTER, pa.string()),
    (CD_CATEGORIA_MASTER_INTERNET, pa.string()),
    (CD_CLASSETERAP, pa.string()),
    (CD_CLASSIF_COMERCIAL, pa.string()),
    (CD_CLASSIF_PROD, pa.string()),
    (CD_CLAS_FISCAL, pa.string()),
    (CD_CLAS_FISCAL_TIPI, pa.string()),
    (CD_COMERCIALIZACAO, pa.string()),
    (CD_COMPOSICAO, pa.string()),
    (CD_DCB, pa.string()),
    (CD_FORMA_FARMA, pa.string()),
    (CD_FORNECEDOR_ULT_ENTRADA, pa.string()),
    (CD_GRCOMPRAS, pa.string()),
    (CD_GRFICHA, pa.string()),
    (CD_GRUPO, pa.string()),
    (CD_GRUPO_INTERNET, pa.string()),
    (CD_GRUPO_REMARCACAO, pa.string()),
    (CD_GRUPO_REMARCACAO_INT, pa.string()),
    (CD_INDICA, pa.string()),
    (CD_LISTA_PIS_COFINS, pa.string()),
    (CD_LOCAL_PRODUTO, pa.string()),
    (CD_LOCAL_PRODUTO_LOJA, pa.string()),
    (CD_MENSAGEM, pa.string()),
    (CD_OPERADOR, pa.string()),
    (CD_PRINCIPIO_ATIVO_COMPOSTO, pa.string()),
    (CD_PRODUTO, pa.string()),
    (CD_PRODUTO_COMPACTO, pa.string()),
    (CD_PRODUTO_LINHA, pa.string()),
    (CD_PRODUTO_MARCA, pa.string()),
    (CD_PRODUTO_ORIGEM, pa.string()),
    (CD_PRODUTO_SAZONALIDADE, pa.string()),
    (CD_PRODUTO_SEM_ETIQUETA, pa.string()),
    (CD_PRODUTO_STATUS, pa.string()),
    (CD_PRODUTO_SUBMARCA, pa.string()),
    (CD_PRODUTO_VLR_AGREGADO, pa.string()),
    (CD_PSICOTROPICOS, pa.string()),
    (CD_SAL, pa.string()),
    (CD_SETOR_SEPARACAO, pa.string()),
    (CD_SUBGRUPO, pa.string()),
    (CD_SUBGRUPO_INTERNET, pa.string()),
    (CD_SUB_CATEGORIA, pa.string()),
    (CD_SUB_CATEGORIA_INTERNET, pa.string()),
    (CD_TARJA, pa.string()),
    (CD_TIPO_ETQ, pa.string()),
    (CD_UNIDADE, pa.string()),
    (CD_UNIDADE_FORMA_QUANTIDADE, pa.string()),
    (CD_USO, pa.string()),
    (CL_CURVA_FIN, pa.string()),
    (CL_CURVA_FIS, pa.string()),
    (DS_PRODUTO, pa.string()),
    (DS_PRODUTO_ANVISA, pa.string()),
    (DS_PRODUTO_COR_RGB, pa.string()),
    (DS_PRODUTO_DET, pa.string()),
    (DS_PRODUTO_FONETICA, pa.string()),
    (DS_PRODUTO_SIAC, pa.string()),
    (DS_PRODUTO_TECNICA, pa.string()),
    (DT_ATUALIZACAO_PDV, pa.string()),
    (DT_BLOQUEIA_ENCOMENDA, pa.string()),
    (DT_CADASTRO, pa.string()),
    (DT_PRIMEIRA_ENTRADA, pa.string()),
    (DT_PRODUTO_INATIVO, pa.string()),
    (DT_REGULARIZACAO_STATUS, pa.string()),
    (DT_ULT_ALT_CLAS_FISCAL, pa.string()),
    (DT_ULT_ENTRADA, pa.string()),
    (DT_ULT_REMARC, pa.string()),
    (DT_VENCIMENTO_REGISTRO_ANVISA, pa.string()),
    (FL_AVULSO, pa.string()),
    (FL_BLOQUEIA_ENCOMENDA, pa.string()),
    (FL_BMPO, pa.string()),
    (FL_BSPO, pa.string()),
    (FL_CADASTRO_GENERICO, pa.string()),
    (FL_CADASTRO_PROVISORIO, pa.string()),
    (FL_CARTELADO_NAO_NEGOCIADO, pa.string()),
    (FL_COMPRA_FABRICANTE, pa.string()),
    (FL_CUPONAVEL, pa.string()),
    (FL_DELETADO, pa.string()),
    (FL_DESATIVACAO_PSICO, pa.string()),
    (FL_ENVIO_SNGPC, pa.string()),
    (FL_ESPACO_BELEZA, pa.string()),
    (FL_EXIGE_CUPOM, pa.string()),
    (FL_HABILITA_EAN, pa.string()),
    (FL_INATIVO, pa.string()),
    (FL_INTERNET, pa.string()),
    (FL_JATEAVEL, pa.string()),
    (FL_LOTE_FATURAMENTO, pa.string()),
    (FL_MARCA_PROPRIA, pa.string()),
    (FL_OTC, pa.string()),
    (FL_PSICO_BALANCO_CONTR_ESP, pa.string()),
    (FL_TRANSMITIDO, pa.string()),
    (MD_ALTURA_CM double , pa.string()),
    (MD_COMPRIMENTO_CM double , pa.string()),
    (MD_LARGURA_CM double , pa.string()),
    (NR_REGISTRO_ANVISA, pa.string()),
    (NR_SHELF_LIFE, pa.string()),
    (NR_ULT_LOTE, pa.string()),
    (PC_ICMS_ENTRADA, pa.string()),
    (PC_IPI, pa.string()),
    (QT_DOSAGEM double , pa.string()),
    (QT_EMBALAGEM, pa.string()),
    (QT_PESO double , pa.string()),
    (QT_PRODUTO_COMPACTO, pa.string()),
    (QT_UNID_MAX_ABAST, pa.string()),
    (VL_CUSTO_MEDIO double , pa.string()),
    (VL_CUSTO_MEDIO_ANTERIOR, pa.string()),
    (VL_CUSTO_MEDIO_SEM_TRIB, pa.string()),
    (VL_CUSTO_RAIA double , pa.string()),
    (VL_CUSTO_ULT_ENTRADA, pa.string()),
    (VL_DESCONTO_FINANCEIRO_MEDIO, pa.string()),
    (VL_ICMS_MEDIO_ENTRADA, pa.string())])

schema = pyarrow.schema([
    ('cd_canal_venda', pyarrow.int64()),
    ( 'ds_canal_venda', pyarrow.string()),
    ( 'dt_process_stage', pyarrow.timestamp())
    ])


pa.schema([('cd_canal_venda', pa.int64()),( 'ds_canal_venda', pa.string(),( 'dt_process_stage', pyarrow.date64())])
--------------

tb canal venda ------

--------------




---------------------------


transformação json to parquet 
append com parquet
arquivo 10 gb



--------------------------------
explorar examples
ververica 


---------------------------
kafka   
___________________________
