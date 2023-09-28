# para correr el Google Cloud
#   8 vCPU
#  16 GB memoria RAM


# limpio la memoria
rm(list = ls()) # remove all objects
gc() # garbage collection

require("data.table")
require("lightgbm")
require("mlflow")


# defino los parametros de la corrida, en una lista, la variable global  PARAM
#  muy pronto esto se leera desde un archivo formato .yaml
PARAM <- list()
PARAM$experimento <- "KA5240"

PARAM$input$dataset <- "./datasets/competencia_02.csv"

# meses donde se entrena el modelo
PARAM$input$training <- c(202101, 202102, 202103, 202104, 202105)
PARAM$input$future <- c(202107) # meses donde se aplica el modelo

PARAM$finalmodel$semilla <- 200177

PARAM$finalmodel$num_iterations <- 857
PARAM$finalmodel$learning_rate <- 0.153978
PARAM$finalmodel$feature_fraction <- 0.749071
PARAM$finalmodel$min_data_in_leaf <- 864
PARAM$finalmodel$num_leaves <- 1012


PARAM$finalmodel$first_metric_only <- TRUE
PARAM$finalmodel$boost_from_average <- TRUE
PARAM$finalmodel$feature_pre_filter <- FALSE
PARAM$finalmodel$verbosity <- -100
PARAM$finalmodel$max_depth <- -1
PARAM$finalmodel$min_gain_to_split <- 0
PARAM$finalmodel$lambda_l1 <- 0
PARAM$finalmodel$lambda_l2 <- 0
PARAM$finalmodel$force_row_wise <- TRUE

PARAM$finalmodel$max_bin <- 31

envios <-13316

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
# Aqui empieza el programa
setwd("~/buckets/b1")

# cargo el dataset donde voy a entrenar
dataset <- fread(PARAM$input$dataset, stringsAsFactors = TRUE)


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
dir.create(paste0("./exp/", PARAM$experimento, "/"), showWarnings = FALSE)

# Establezco el Working Directory DEL EXPERIMENTO
setwd(paste0("./exp/", PARAM$experimento, "/"))


with(mlflow_start_run(), {
  # dejo los datos en el formato que necesita LightGBM
  dtrain <- lgb.Dataset(
    data = data.matrix(dataset[train == 1L, campos_buenos, with = FALSE]),
    label = dataset[train == 1L, clase01]
  )

  # genero el modelo
  # estos hiperparametros  salieron de una laaarga Optmizacion Bayesiana
  modelo <- lgb.train(
    data = dtrain,
    param = list(
      objective = "binary",
      max_bin = PARAM$finalmodel$max_bin,
      learning_rate = PARAM$finalmodel$learning_rate,
      num_iterations = PARAM$finalmodel$num_iterations,
      num_leaves = PARAM$finalmodel$num_leaves,
      min_data_in_leaf = PARAM$finalmodel$min_data_in_leaf,
      feature_fraction = PARAM$finalmodel$feature_fraction,
      seed = PARAM$finalmodel$semilla
    )
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

  tb_entrega[, Predicted := 0L]
  tb_entrega[1:envios, Predicted := 1L]

  fwrite(tb_entrega[, list(numero_de_cliente, Predicted)],
    file = paste0(PARAM$experimento, "_", envios, ".csv"),
    sep = ","
  )

  mlflow_log_param("max_bin", PARAM$finalmodel$max_bin)
  mlflow_log_param("learning_rate", PARAM$finalmodel$learning_rate)
  mlflow_log_param("num_iterations", PARAM$finalmodel$num_iterations)
  mlflow_log_param("num_leaves", PARAM$finalmodel$num_leaves)
  mlflow_log_param("min_data_in_leaf", PARAM$finalmodel$min_data_in_leaf)
  mlflow_log_param("feature_fraction", PARAM$finalmodel$feature_fraction)
  mlflow_log_param("seed", PARAM$finalmodel$semilla)

  mlflow_log_param("envios", envios)
  mlflow_log_model(predictor, "model")
})

cat("\n\nLa generacion de los archivos para Kaggle ha terminado\n")
