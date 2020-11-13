
parse.rema <- function(X, params, dirAWS, dirUP = NULL,
                       upload = TRUE, archive = FALSE)
{
    if(upload) on.exit(ssh::ssh_disconnect(session))

    stn.id <- X$aws
    dirL <- create_dirLoc_aws(dirAWS, "REMA", stn.id)

    if(upload){
        ssh <- readRDS(file.path(dirAWS, "AWS_PARAMS", "data-int.cred"))
        session <- try(do.call(ssh::ssh_connect, ssh$cred), silent = TRUE)

        if(inherits(session, "try-error")){
            logUpload <- file.path(dirL$dirLog, "UPLOAD_LOG.txt")
            msg <- paste(session, "Unable to connect to data-int server\n", stn.id)
            format.out.msg(msg, logUpload)
            
            upload <- FALSE
        }else{
            dirU <- create_dirUp_aws(session, dirUP, "REMA", stn.id)
        }
    }

    ########################

    logAWS <- file.path(dirAWS, "AWS_DATA", "REMA", "LOG", "AWS_LOG.txt")

    daty <- strptime(X$data[, 1], "%Y-%m-%d %H:%M", tz = "Africa/Kigali")
    nadates <- is.na(daty)
    if(all(nadates)){
        msg <- paste("Invalid dates :", stn.id)
        format.out.msg(msg, logAWS)
        return(NULL)
    }

    if(any(nadates)){
        file.log <- "INVALID_DATES.txt"
        file.loc <- file.path(dirL$logLoc, file.log)
        invalid <- paste0(X$data[nadates, 1], collapse = "; ")
        msg <- paste("AWS :", stn.id, "\n", invalid)
        format.out.msg(msg, file.loc)

        if(upload){
            file.up <- file.path(dirU$logUp, file.log)
            ssh::scp_upload(session, file.loc, to = file.up, verbose = FALSE)
        }
    }

    daty <- daty[!nadates]
    don <- X$data[!nadates, , drop = FALSE]

    oldVars <- NULL
    if(file.exists(dirL$info)){
        info <- readRDS(dirL$info)
        infostart <- strptime(info$start, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
        infoend <- strptime(info$end, "%Y%m%d%H%M%S", tz = "Africa/Kigali")

        if(!archive){
            idaty <- daty >= infoend - 3600
            if(!any(idaty)){
                updated <- FALSE
                return("no.update")
            }

            updated <- TRUE
            daty <- daty[idaty]
            don <- don[idaty, , drop = FALSE]
        }

        oldVars <- info$vars
    }

    ########################

    xhead <- as.character(don[1, ])

    if(length(grep("17-", xhead)) == 0){
        msg <- paste("No weather variables found :", stn.id)
        format.out.msg(msg, logAWS)
        return(NULL)
    }

    outdata <- list()
    for(ii in seq(nrow(params))){
        varid <- params[ii, "code"]
        if(varid %in% xhead)
            outdata[[params[ii, "Variable"]]] <- as.numeric(don[, which(xhead == varid) + 1])
    }

    if(length(outdata) == 0){
        msg <- paste("Unable to parse, no variable found :", stn.id)
        format.out.msg(msg, logAWS)
        return(NULL)
    }

    nomVars <- lapply(names(outdata), function(n){
        switch(n, "RR" = "Tot", "FFmax" = "Max", "Ave")
    })
    names(nomVars) <- names(outdata)
    oldVars <- merge.all.variables(oldVars, nomVars)

    parsL <- doparallel.cond(archive & length(daty) > 200)

    retLoop <- cdtforeach(seq_along(daty), parsL, FUN = function(tt){
        temps <- format(daty[tt], "%Y%m%d%H%M%S", tz = "Africa/Kigali")
        x <- lapply(outdata, "[[", tt)
        res_dat <- lapply(seq_along(x), function(j){
            df <- data.frame(x[[j]])
            names(df) <- switch(names(x[j]), "RR" = "Tot",
                                "FFmax" = "Max", "Ave")
            df
        })
        names(res_dat) <- names(x)

        ina <- sapply(res_dat, is.na)
        if(all(ina)){
            file.log <- paste0(substr(temps, 1, 12), "_nodata.txt")
            log.loc <- file.path(dirL$logLoc, file.log)
            msg <- paste("AWS :", stn.id, "\n", "No data for :", temps)
            format.out.msg(msg, log.loc, FALSE)
            if(upload){
                log.up <- file.path(dirU$logUp, file.log)
                return(list(log = c(log.loc, log.up), data = NULL))
            }else{
                return(0)
            }
        }

        res_dat <- res_dat[!ina]
        out <- list(date = temps, data = res_dat)

        file.out <- paste0(temps, ".rds")
        data.loc <- file.path(dirL$dataLoc, file.out)
        saveRDS(out, file = data.loc)

        if(upload){
            data.up <- file.path(dirU$dataUp, file.out)
            return(list(data = c(data.loc, data.up), log = NULL))
        }else{
            return(0)
        }
    })

    if(upload){
        upld <- lapply(retLoop, function(x){
            if(!is.null(x$log))
                ssh::scp_upload(session, x$log[1], to = x$log[2], verbose = FALSE)
            if(!is.null(x$data))
                ssh::scp_upload(session, x$data[1], to = x$data[2], verbose = FALSE)
            return(0)
        })
    }

    ########################

    daty0 <- format(daty, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
    ndt <- length(daty0)
    if(!file.exists(dirL$info)){
        timestep <- as.numeric(names(which.max(table(diff(daty)))))
        info <- list(id = stn.id, start = daty0[1],
                     end = daty0[ndt], vars = oldVars,
                     tstep = timestep, updated = TRUE)
    }else{
        if(archive){
            if(infostart > daty[1]) info$start <- daty0[1]
            if(infoend < daty[ndt]) info$end <- daty0[ndt]
        }else{
            info$end <- daty0[ndt]
            info$updated <- updated
        }

        info$vars <- oldVars
    }

    saveRDS(info, file = dirL$info)
    if(upload)
        ssh::scp_upload(session, dirL$info, to = dirU$info, verbose = FALSE)

    return("updated")
}
