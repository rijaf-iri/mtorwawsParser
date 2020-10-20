
get.rema <- function(timeR, dirELAB, dirAWS, dirUP = NULL,
                     upload = TRUE, archive = FALSE)
{
    logAWS <- file.path(dirAWS, "AWS_DATA", "REMA", "LOG", "AWS_LOG.txt")
    dirPARS <- file.path(dirAWS, "AWS_PARAMS")

    listAWS <- list.files(dirELAB, "gateway_data_[[:digit:]]+\\.txt")
    listAWS <- trimws(gsub("[^[:digit:]]", "", listAWS))

    liststn <- file.path(dirPARS, "REMA_list.txt")
    REMAWS <- utils::read.table(liststn)
    REMAWS <- trimws(as.character(REMAWS[, 1]))
    listAWS <- listAWS[listAWS %in% REMAWS]

    if(length(listAWS) == 0){
        format.out.msg("No AWS file found", logAWS)
        return(0)
    }

    ###########
    pars <- utils::read.table(file.path(dirPARS, "REMA_params.csv"), header = TRUE, sep = ",",
                              colClasses = "character", stringsAsFactors = FALSE)
    params <- pars[trimws(pars$Variable) != "", c("code", "Variable"), drop = FALSE]

    ###########

    ret.aws <- lapply(listAWS, function(aws){
        awsfile <- file.path(dirELAB, paste0("gateway_data_", aws, ".txt"))
        nL <- R.utils::countLines(awsfile)

        lastdaty <- try(read.rema.elab(awsfile, skip = nL - 3), silent = TRUE)
        if(inherits(lastdaty, "try-error")){
            msg <- paste("Unable to read :", aws, basename(awsfile))
            format.out.msg(msg, logAWS)
            return(-1)
        }

        lastdaty <- strptime(lastdaty[, 1], "%Y-%m-%d %H:%M", tz = "Africa/Kigali")
        lastdaty <- lastdaty[!is.na(lastdaty)]

        if(length(lastdaty) == 0){
            msg <- paste("Ambiguous file format :", aws, basename(awsfile))
            format.out.msg(msg, logAWS)
            return(-1)
        }

        lastdaty <- lastdaty[length(lastdaty)]

        if(archive){
            firstdaty <- read.rema.elab(awsfile, nrows = 3)
            firstdaty <- strptime(firstdaty[, 1], "%Y-%m-%d %H:%M", tz = "Africa/Kigali")
            firstdaty <- firstdaty[!is.na(firstdaty)]
            firstdaty <- firstdaty[1]

            if(!any(timeR >= firstdaty & timeR <= lastdaty)){
                msg <- paste("No data between", timeR[1], 'and', timeR[2],
                             'for', aws, basename(awsfile))
                format.out.msg(msg, logAWS)
                return(0)
            }

            dfmin <- difftime10Min(firstdaty, lastdaty) + 1
            if(dfmin == nL){
                nrows <- -1
                skip <- 0
                if(timeR[1] >= firstdaty)
                    skip <- floor(difftime10Min(firstdaty, timeR[1]))

                if(timeR[2] <= lastdaty){
                    rtail <- ceiling(difftime10Min(timeR[2], lastdaty))
                    nrows <- nL - rtail - skip
                }

                don <- read.rema.elab(awsfile, nrows = nrows, skip = skip)
            }else{
                don <- read.rema.elab(awsfile)
                daty <- strptime(don[, 1], "%Y-%m-%d %H:%M", tz = "Africa/Kigali")
                ix <- daty >= timeR[1] & daty <= timeR[2]

                don <- don[ix, , drop = FALSE]
            }
        }else{
            dfmin <- difftime10Min(timeR[1], lastdaty)
            if(dfmin <= 0){
                msg <- paste("No new data, no update :", aws, basename(awsfile))
                format.out.msg(msg, logAWS)
                return(0)
            }

            skip <- nL - ceiling(dfmin) - 1
            don <- read.rema.elab(awsfile, skip = skip)
        }

        X <- list(aws = aws, data = don)
        out <- try(parse.rema(X, params, dirAWS, dirUP, upload, archive), silent = TRUE)

        if(inherits(out, "try-error")){
            msg <- paste(out, "Unable to process :", aws, basename(awsfile))
            format.out.msg(msg, logAWS)
            return(-1)
        }else{
            if(!is.null(out)){
                if(out == "no.update"){
                    msg <- paste("No new data, no update :", aws, basename(awsfile))
                    format.out.msg(msg, logAWS)
                    return(0)
                }
                if(out == "updated"){
                    msg <- paste("Updated :", aws, basename(awsfile))
                    format.out.msg(msg, logAWS)
                    return(0)
                }
            }
        }
    })

    return(0)
}
