# para correr el Google Cloud
#   8 vCPU
#  64 GB memoria RAM


# limpio la memoria
rm(list = ls()) # remove all objects
gc() # garbage collection

############# Start mlflow ################
#In a terminal: mlflow server --backend-store-uri ~/buckets/b1/mlruns
library(mlflow)
require("carrier")

Sys.setenv(MLFLOW_BIN=system("which mlflow"))
Sys.setenv(MLFLOW_PYTHON_BIN=system("which python"))
mlflow_set_tracking_uri("sqlite:///database/mlruns.db")

require("data.table")
require("lightgbm")


# defino los parametros de la corrida, en una lista, la variable global  PARAM
#  muy pronto esto se leera desde un archivo formato .yaml
PARAM <- list()
PARAM$experimento <- "KA8240"

PARAM$input$dataset <- "./datasets/processed/competencia_03.csv"

# meses donde se entrena el modelo
PARAM$input$training <- c(
  201901,
  201902,
  201903,
  201904,
  201905,
  201906,
  201907,
  201908,
  201908,
  201909,
  201910,
  201911,
  201912,
  202001,
  202002,
  202010,
  202011,
  202012,
  202101,
  202102,
  202103,
  202104,
  202105,
  202106,
  202107
)
PARAM$input$future <- c(202109) # meses donde se aplica el modelo

PARAM$finalmodel$semilla <- 210913

# hiperparametros intencionalmente NO optimos
PARAM$finalmodel$optim$num_iterations <- 821
PARAM$finalmodel$optim$learning_rate <- 0.121842089716811
PARAM$finalmodel$optim$feature_fraction <- 	0.716726674084202
PARAM$finalmodel$optim$min_data_in_leaf <- 36896
PARAM$finalmodel$optim$num_leaves <- 797

SEEDS <- c(100057, 101183, 195581, 210913, 219761, 221123, 221131, 221137, 221147, 221153, 221159)


#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
# Aqui empieza el programa
setwd("~/buckets/b1")

# cargo el dataset donde voy a entrenar
dataset <- fread(PARAM$input$dataset, stringsAsFactors = TRUE)


# Catastrophe Analysis  -------------------------------------------------------
# deben ir cosas de este estilo
#   dataset[foto_mes == 202006, active_quarter := NA]

# Data Drifting
# por ahora, no hago nada


# Feature Engineering Historico  ----------------------------------------------
#   aqui deben calcularse los  lags y  lag_delta
#   Sin lags no hay paraiso ! corta la bocha
#   https://rdrr.io/cran/data.table/man/shift.html


#--------------------------------------

# paso la clase a binaria que tome valores {0,1}  enteros
# set trabaja con la clase  POS = { BAJA+1, BAJA+2 }
# esta estrategia es MUY importante
dataset[, clase01 := ifelse(clase_ternaria %in% c("BAJA+2", "BAJA+1"), 1L, 0L)]

#--------------------------------------

# los campos que se van a utilizar
campos_buenos <- setdiff(colnames(dataset), c("clase_ternaria", "clase01"))

#--------------------------------------


# establezco donde entreno
dataset[, train := 0L]
dataset[foto_mes %in% PARAM$input$training, train := 1L]

#--------------------------------------
# creo las carpetas donde van los resultados
# creo la carpeta donde va el experimento
dir.create("./exp/", showWarnings = FALSE)


# dejo los datos en el formato que necesita LightGBM
dtrain <- lgb.Dataset(
  data = data.matrix(dataset[train == 1L, campos_buenos, with = FALSE]),
  label = dataset[train == 1L, clase01]
)

for (SEED in SEEDS) {

  # Hiperparametros FIJOS de  lightgbm
  PARAM$finalmodel$lgb_basicos <- list(
    boosting = "gbdt", # puede ir  dart  , ni pruebe random_forest
    objective = "binary",
    metric = "custom",
    first_metric_only = TRUE,
    boost_from_average = TRUE,
    feature_pre_filter = FALSE,
    force_row_wise = TRUE, # para reducir warnings
    verbosity = -100,
    max_depth = -1L, # -1 significa no limitar,  por ahora lo dejo fijo
    min_gain_to_split = 0.0, # min_gain_to_split >= 0.0
    min_sum_hessian_in_leaf = 0.001, #  min_sum_hessian_in_leaf >= 0.0
    lambda_l1 = 0.0, # lambda_l1 >= 0.0
    lambda_l2 = 0.0, # lambda_l2 >= 0.0
    max_bin = 31L, # lo debo dejar fijo, no participa de la BO

    bagging_fraction = 1.0, # 0.0 < bagging_fraction <= 1.0
    pos_bagging_fraction = 1.0, # 0.0 < pos_bagging_fraction <= 1.0
    neg_bagging_fraction = 1.0, # 0.0 < neg_bagging_fraction <= 1.0
    is_unbalance = FALSE, #
    scale_pos_weight = 1.0, # scale_pos_weight > 0.0

    drop_rate = 0.1, # 0.0 < neg_bagging_fraction <= 1.0
    max_drop = 50, # <=0 means no limit
    skip_drop = 0.5, # 0.0 <= skip_drop <= 1.0

    extra_trees = TRUE, # Magic Sauce

    seed = SEED, # semilla
  )
  dir.create(paste0("./exp/", SEED, "/",  PARAM$experimento, "/"), showWarnings = FALSE)

  # Establezco el Working Directory DEL EXPERIMENTO
  setwd(paste0("./exp/", SEED, "/",  PARAM$experimento, "/"))

  # genero el modelo
  param_completo <- c(
    PARAM$finalmodel$lgb_basicos,
    PARAM$finalmodel$optim
  )


  with(mlflow_start_run(), {
    modelo <- lgb.train(
      data = dtrain,
      param = param_completo,
    )

    #--------------------------------------
    # ahora imprimo la importancia de variables
    tb_importancia <- as.data.table(lgb.importance(modelo))
    archivo_importancia <- "impo.txt"

    fwrite(tb_importancia,
      file = archivo_importancia,
      sep = "\t"
    )

    #--------------------------------------


    # aplico el modelo a los datos sin clase
    dapply <- dataset[foto_mes == PARAM$input$future]

    # aplico el modelo a los datos nuevos
    prediccion <- predict(
      modelo,
      data.matrix(dapply[, campos_buenos, with = FALSE])
    )

    # genero la tabla de entrega
    tb_entrega <- dapply[, list(numero_de_cliente, foto_mes)]
    tb_entrega[, prob := prediccion]

    # grabo las probabilidad del modelo
    fwrite(tb_entrega,
      file = "prediccion.txt",
      sep = "\t"
    )

    # ordeno por probabilidad descendente
    setorder(tb_entrega, -prob)

    # genero archivos con los  "envios" mejores
    # deben subirse "inteligentemente" a Kaggle para no malgastar submits
    # si la palabra inteligentemente no le significa nada aun
    # suba TODOS los archivos a Kaggle
    # espera a la siguiente clase sincronica en donde el tema sera explicado

    cortes <- seq(8000, 15000, by = 500)
    for (envios in cortes) {
      tb_entrega[, Predicted := 0L]
      tb_entrega[1:envios, Predicted := 1L]

      fwrite(tb_entrega[, list(numero_de_cliente, Predicted)],
        file = paste0(PARAM$experimento, "_", envios, ".csv"),
        sep = ","
      )
    }

    lgb.save(modelo, file = "modelo.RData")

    mlflow_log_param("learning_rate", PARAM$finalmodel$optim$learning_rate)
    mlflow_log_param("num_iterations", PARAM$finalmodel$optim$num_iterations)
    mlflow_log_param("feature_fraction", PARAM$finalmodel$optim$feature_fraction)
    mlflow_log_param("num_leaves", PARAM$finalmodel$optim$num_leaves)
    mlflow_log_param("min_data_in_leaf", PARAM$finalmodel$optim$min_data_in_leaf)
    mlflow_log_param("seed", SEED)
    mlflow_save_model(modelo, "model")

  })

  cat("\n\nLa generacion de los archivos para Kaggle ha terminado\n")
}
