
parse.lsi.elog <- function(X, dirAWS, dirUP = NULL,
                           upload = TRUE, archive = FALSE)
{
    if(upload) on.exit(ssh::ssh_disconnect(session))

    stn.id <- X$FactorySerialNumber[1]
    dirL <- create_dirLoc_aws(dirAWS, "LSI-ELOG", stn.id)

    if(upload){
        ssh <- readRDS(file.path(dirAWS, "AWS_PARAMS", "data-int.cred"))
        session <- try(do.call(ssh::ssh_connect, ssh$cred), silent = TRUE)

        if(inherits(session, "try-error")){
            logUpload <- file.path(dirL$dirLog, "UPLOAD_LOG.txt")
            msg <- paste(session, "Unable to connect to data-int server\n", stn.id)
            format.out.msg(msg, logUpload)

            upload <- FALSE
        }else{
            dirU <- create_dirUp_aws(session, dirUP, "LSI-ELOG", stn.id)
        }
    }

    ########################

    daty <- format(X$ElaborationDate, "%Y%m%d%H%M%S", tz = "Africa/Kigali")

    oldVars <- NULL
    if(file.exists(dirL$info)){
        info <- readRDS(dirL$info)
        infostart <- strptime(info$start, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
        infoend <- strptime(info$end, "%Y%m%d%H%M%S", tz = "Africa/Kigali")

        if(!archive){
            daty0 <- strptime(daty, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
            idaty <- daty0 >= infoend
            if(!any(idaty)){
                updated <- FALSE
                return("no.update")
            }

            updated <- TRUE
            daty <- daty[idaty]
            X <- X[idaty, , drop = FALSE]
        }

        oldVars <- info$vars
    }

    ########################

    lpdaty <- split(seq_along(daty), daty)
    parsL <- doparallel.cond(archive & length(lpdaty) > 200)

    retLoop <- cdtforeach(seq_along(lpdaty), parsL, FUN = function(jj){
        ix <- lpdaty[[jj]]
        xval <- X[ix, , drop = FALSE]
        temps <- daty[ix][1]

        ret <- split.lsi.elog(xval, temps, oldVars, 
                              dirL$dataLoc, dirL$logLoc, stn.id,
                              dirU$dataUp, dirU$logUp, session, upload)

        return(ret)
    })

    ierror <- sapply(retLoop, '[[', 'error')

    daty <- names(lpdaty)[!ierror]
    daty <- daty[order(daty)]
    ndt <- length(daty)

    mrgVars <- lapply(retLoop, '[[', 'nomVars')
    for(jj in seq_along(mrgVars))
        oldVars <- merge.all.variables(oldVars, mrgVars[[jj]])

    if(upload){
        uploadfiles <- lapply(retLoop, '[[', 'upload')
        upld <- lapply(uploadfiles, function(x){
            if(!is.null(x$log))
                ssh::scp_upload(session, x$log[1], to = x$log[2], verbose = FALSE)
            if(!is.null(x$data))
                ssh::scp_upload(session, x$data[1], to = x$data[2], verbose = FALSE)
        })
    }

    ########################

    if(!file.exists(dirL$info)){
        daty0 <- strptime(daty, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
        timestep <- as.numeric(names(which.max(table(diff(daty0)))))
        info <- list(id = stn.id, start = daty[1],
                     end = daty[ndt], vars = oldVars,
                     tstep = timestep, updated = TRUE)
    }else{
        if(archive){
            daty0 <- strptime(daty, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
            if(infostart > daty0[1]) info$start <- daty[1]
            if(infoend < daty0[ndt]) info$end <- daty[ndt]
        }else{
            info$end <- daty[ndt]
            info$updated <- updated
        }

        info$vars <- oldVars
    }

    saveRDS(info, file = dirL$info)
    if(upload)
        ssh::scp_upload(session, dirL$info, to = dirU$info, verbose = FALSE)

    return("updated")
}

split.lsi.elog <- function(xval, temps, oldVars, 
                           dirDataLoc, dirLogLoc, stnID,
                           dirDataUp, dirLogUp, session, upload)
{
    lpvars <- split(seq_along(xval$VarName), xval$VarName)
    res_dat <- lapply(lpvars, function(ivr){
        x <- xval[ivr, , drop = FALSE]
        obs <- round(matrix(x$ElaborationValue, nrow = 1), 3)
        nom <- as.character(x$ElabTypeString)
        # vprc <- x$ValidPercentage[1]
        vprc <- max(x$ValidPercentage)
        y <- data.frame(obs, vprc)
        names(y) <- c(nom, "ValidDataPerc")
        y
    })

    nomVars <- lapply(res_dat, names)

    res_dat <- lapply(res_dat, function(x){
        ina <- is.na(x)
        if(all(ina)) return(NULL)
        y <- x[, !ina, drop = FALSE]
        if(ncol(y) == 1)
            if(names(y) == "ValidDataPerc") return(NULL)
        y
    })

    inull <- sapply(res_dat, is.null)
    if(all(inull)){
        file.log <- paste0(substr(temps, 1, 12), "_nodata.txt")
        log.loc <- file.path(dirLogLoc, file.log)
        msg <- paste("AWS :", stnID, "\n", "No data for :", temps)
        format.out.msg(msg, log.loc, FALSE)

        if(upload){
            log.up <- file.path(dirLogUp, file.log)
            data.up <- file.path(dirDataUp, file.out)
            return(list(error = TRUE,
                        nomVars = nomVars,
                        upload = list(log = c(log.loc, log.up), data = NULL))
                    )
        }else{
            return(list(error = TRUE, nomVars = nomVars, upload = NULL))
        }
    }
    res_dat <- res_dat[!inull]
    out <- list(date = temps, data = res_dat)

    file.out <- paste0(temps, ".rds")
    data.loc <- file.path(dirDataLoc, file.out)
    saveRDS(out, file = data.loc)

    if(upload){
        data.up <- file.path(dirDataUp, file.out)
        return(list(error = FALSE,
                    nomVars = nomVars,
                    upload = list(data = c(data.loc, data.up), log = NULL))
                )
    }else{
        return(list(error = FALSE, nomVars = nomVars, upload = NULL))
    }
}
