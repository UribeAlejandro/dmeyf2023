# limpio la memoria
rm(list = ls()) # remove all objects
gc() # garbage collection

require("data.table")
require("rlist")

require("rpart")
require("parallel")


ArbolSimple <- function(dtrain, dapply, pesos) {
    param <- list()
    param$cp <- -1
    param$minsplit <- 753
    param$minbucket <- 376
    param$corte <- 8223
    param$maxdepth <- 9

    modelo <- rpart("clase_binaria ~ . - clase_ternaria",
        data = dtrain,
        xval = 0,
        control = param,
        weights = pesos
    )


    prediccion <- predict(modelo,
        dapply,
        type = "prob"
    )

    prob_baja <- prediccion[, "POS"]

    tablita <- copy(dapply[, list(numero_de_cliente)])
    tablita[, prob := prob_baja]
    setorder(tablita, -prob)

    # grabo el submit a Kaggle
    tablita[, Predicted := 0L]
    tablita[1:param$corte, Predicted := 1L]

    nom_submit <- paste0("./datasets/processed/", "final_kaggle_1.csv")
    fwrite(tablita[, list(numero_de_cliente, Predicted)],
        file = nom_submit,
        sep = ","
    )
}

dataset <- fread("./datasets/interim/competencia_01.csv")
dataset$clase_ternaria <- as.factor(dataset$clase_ternaria)

# dataset$var1 <- dataset$ctrx_quarter < 18
dataset$var1 <- dataset$mcuentas_saldo < -1388.2

# defino la clase_binaria2
dataset[, clase_binaria := ifelse(clase_ternaria == "CONTINUA", "NEG", "POS")]

dtrain <- dataset[foto_mes == 202103]
dapply <- dataset[foto_mes == 202105]

pesos <- copy(dtrain[, ifelse(clase_ternaria == "CONTINUA", 1.0, 100.0)])



ArbolSimple(dtrain, dapply, pesos)
