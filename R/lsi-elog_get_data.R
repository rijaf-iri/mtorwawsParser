
get.lsi.elog <- function(timeR, dirAWS, dirUP = NULL,
                         upload = TRUE, archive = FALSE)
{
    logAWS <- file.path(dirAWS, "AWS_DATA", "LSI-ELOG", "LOG", "AWS_LOG.txt")
    gidas <- readRDS(file.path(dirAWS, "AWS_PARAMS", "gidas.con"))
    con <- RODBC::odbcDriverConnect(gidas$connection, readOnlyOptimize = TRUE)

    query1 <- paste0("SELECT * FROM Core.vFlatCoreData WHERE ElaborationDate BETWEEN '",
                     timeR[1], "' AND '", timeR[2], "'")
    data_table <- RODBC::sqlQuery(con, query1)

    if(is.null(dim(data_table))){
        msg <- "An error occurred when connecting to Gidas database, no update"
        format.out.msg(msg, logAWS)
        return(1)
    }

    id_aws <- unique(data_table$FactorySerialNumber)

    if(length(id_aws) == 0){
        msg <- "No new data for all AWS, no update"
        format.out.msg(msg, logAWS)
        return(0)
    }
 
    query2 <- paste0("SELECT * FROM Core.vInstrumentMeasures_GetAll WHERE FactorySerialNumber IN (",
                     paste0(id_aws, collapse = ","), ")")
    variables <- RODBC::sqlQuery(con, query2)
    valConf <- RODBC::sqlQuery(con, 'SELECT * FROM Core.ValueConfiguration')
    params <- RODBC::sqlQuery(con, 'SELECT * FROM Core.ElabTypeList')
    RODBC::odbcCloseAll()

    ##
    data_table <- data_table[, c('FactorySerialNumber', 'ValueConfigurationID',
                                 'ElaborationDate', 'ElaborationValue', 'ValidPercentage')]
    variables <- variables[, c('FactorySerialNumber', 'ValueConfigurationID',
                               'MeasureName', 'MeasureUnit', 'MeasureId')]
    valConf <- valConf[, c('ValueConfigurationID', 'ElaborationType')]
    params <- params[, c('IdElabType', 'ElabTypeString')]

    ###
    ivar <- match(data_table$ValueConfigurationID, variables$ValueConfigurationID)
    don <- cbind(data_table, variables[ivar, ])
    don <- don[, !duplicated(names(don))]
    ival <- match(don$ValueConfigurationID, valConf$ValueConfigurationID)
    don <- cbind(don, valConf[ival, ])
    don <- don[, !duplicated(names(don))]
    ipar <- match(don$ElaborationType, params$IdElabType)
    don <- cbind(don, params[ipar, ])
    # don$ElaborationValue[don$ElaborationValue == -999999.] <- NA
    don$ElaborationValue[don$ElaborationValue < -9999.] <- NA

    ###########

    vars <- utils::read.table(file.path(dirAWS, "AWS_PARAMS", "LSI_ELOG_params.csv"),
                              header = TRUE, sep = ",", colClasses = "character",
                              stringsAsFactors = FALSE)

    iv <- match(as.character(don$MeasureName), trimws(as.character(vars$MeasureName)))
    don$VarName <- trimws(as.character(vars$IdVar))[iv]

    ret.aws <- lapply(id_aws, function(id){
        x <- don[don$FactorySerialNumber == id, ]
        out <- try(parse.lsi.elog(x, dirAWS, dirUP, upload, archive), silent = TRUE)

        if(inherits(out, "try-error")){
            msg <- paste(out, "Unable to process :", x$FactorySerialNumber[1])
            format.out.msg(msg, logAWS)
            return(-1)
        }else{
            if(is.null(out)){
                msg <- paste("No data or all data are missing, no update :", x$FactorySerialNumber[1])
                format.out.msg(msg, logAWS)
                return(1)
            }else{
                if(out == "no.update"){
                    msg <- paste("No new data, no update :", x$FactorySerialNumber[1])
                    format.out.msg(msg, logAWS)
                    return(0)
                }
                if(out == "updated"){
                    msg <- paste("Updated :", x$FactorySerialNumber[1])
                    format.out.msg(msg, logAWS)
                    return(0)
                }
            }
        }
    })

    return(0)
}
