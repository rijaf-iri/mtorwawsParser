
parse.lsi.xlog <- function(X, params, dirLSIXLOG, dirAWS,
                           dirUP = NULL, upload = TRUE,
                           archive = FALSE)
{
    if(upload) on.exit(ssh::ssh_disconnect(session))

    stn.id <- X$id
    stn.name <- X$name
    dirL <- create_dirLoc_aws(dirAWS, "LSI-XLOG", stn.id)

    if(upload){
        ssh <- readRDS(file.path(dirAWS, "AWS_PARAMS", "data-int.cred"))
        session <- try(do.call(ssh::ssh_connect, ssh$cred), silent = TRUE)

        if(inherits(session, "try-error")){
            logUpload <- file.path(dirL$dirLog, "UPLOAD_LOG.txt")
            msg <- paste(session, "Unable to connect to data-int server\n", stn.name, stn.id)
            format.out.msg(msg, logUpload)

            upload <- FALSE
        }else{
            dirU <- create_dirUp_aws(session, dirUP, "LSI-XLOG", stn.id)
        }
    }

    ########################

    oldVars <- NULL
    files.aws <- X$files
    if(file.exists(dirL$info)){
        info <- readRDS(dirL$info)
        infostart <- strptime(info$start, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
        infoend <- strptime(info$end, "%Y%m%d%H%M%S", tz = "Africa/Kigali")

        if(!archive){
            idaty <- X$daty >= infoend
            if(!any(idaty)){
                updated <- FALSE
                return("no.update")
            }

            updated <- TRUE
            files.aws <- files.aws[idaty]
        }
        oldVars <- info$vars
    }

    ########################

    writeUpload <- function(msg, stn, errorxT){
        file.log <- paste0(substr(stn, 7, 18), errorxT)
        file.loc <- file.path(dirL$logLoc, file.log)
        file.up <- file.path(dirU$logUp, file.log)
        ret <- c(paste("Time:", Sys.time(), "\n"), msg, "\n")
        cat(ret, file = file.loc)
        if(upload)
            ssh::scp_upload(session, file.loc, to = file.up, verbose = FALSE)
    }

    ########################

    ## replace parallel on archive mode
    parsL <- doparallel.cond(archive & length(files.aws) > 200)

    retLoop <- cdtforeach(seq_along(files.aws), parsL, FUN = function(jj){
        stn <- files.aws[jj]
        # ret.don <- lapply(files.aws, function(stn){
        ff <- file.path(dirLSIXLOG, stn.name, stn)
        ret <- try(readLines(ff, warn = FALSE, encoding = "UTF-8"), silent = TRUE)
        if(inherits(ret, "try-error")){
            msg <- paste("AWS :", stn.name, "\n",
                         "Unable to read :", stn, "\n", ret)
            writeUpload(msg, stn, "_read.txt")
            return(NULL)
        }

        don <- strsplit(ret, ";|,")
        if(length(don) == 0){
            msg <- paste("AWS :", stn.name, "\n",
                         "Ambiguous format or no data in :", stn)
            writeUpload(msg, stn, "_format.txt")
            return(NULL)
        }

        dd <- lapply(don, function(x){
            x <- trimws(x)
            if(length(x) <= 8){
                msg <- paste("AWS :", stn.name, "\n", "No data in :", stn)
                writeUpload(msg, stn, "_nodata.txt")
                return(NULL)
            }

            if(x[length(x)] != "#"){
                msg <- paste("AWS :", stn.name, "\n", "Data from", stn,
                             "is not terminated normally\n")
                writeUpload(msg, stn, "_format.txt")
                return(NULL)
            }

            id <- x[2]
            daty <- strptime(paste0(x[3:8], collapse = ""), "%H%M%S%d%m%Y", tz = "Africa/Kigali")
            daty <- format(daty, "%Y%m%d%H%M%S", tz = "Africa/Kigali")

            x <- x[-(1:8)]
            x <- x[-length(x)]
            x[x == "-999990.00" | x == "-999999.00" | x == "-9999.0" | x == "-9999" | x == "*"] <- NA
            x <- suppressWarnings(as.numeric(x))
            x <- matrix(x, ncol = 3, byrow = TRUE)

            rd <- rle(x[, 1])
            ird <- duplicated(rd$values)
            if(any(ird)){
                ne <- cumsum(rd$lengths)
                ns <- c(1, ne[-length(ne)] + 1)
                rd <- cbind(ns, ne)
                rd <- rd[ird, , drop = FALSE]
                idx <- do.call(c, lapply(seq(nrow(rd)), function(i) rd[i, 1]:rd[i, 2]))
                x <- x[-idx, , drop = FALSE]
            }

            VARS <- params$var[params$var$ID %in% unique(x[, 1]), , drop = FALSE]
            if(nrow(VARS) == 0){
                msg <- paste("AWS :", stn.name, "\n", "No variables found :", stn)
                writeUpload(msg, stn, "_nodata.txt")
                return(NULL)
            }
 
            outdata <- list()
            for(ii in seq_along(VARS$ID)){
                varid <- VARS$ID[ii]
                varname <- VARS$Variable[ii]
                tmp <- x[x[, 1] %in% varid, , drop = FALSE]
                nom <- params$stat$STAT[match(tmp[, 2], params$stat$ID)]
                tmp <- data.frame(matrix(tmp[, 3], nrow = 1, ncol = length(nom)))
                names(tmp) <- nom
                outdata[[varname]] <- tmp
            }
            list(id = id, date = daty, data = outdata)
        })

        inl <- sapply(dd, is.null)
        dd <- dd[!inl]
        if(length(dd) == 0){
            msg <- paste("AWS :", stn.name, "\n", "Unable to parse :", stn)
            writeUpload(msg, stn, "_read.txt")
            return(NULL)
        }

        id <- do.call(c, lapply(dd, "[[", "id"))
        daty <- do.call(c, lapply(dd, "[[", "date"))
        don <- lapply(dd, '[[', 'data')
        ina <- sapply(daty, is.na)

        if(all(ina)){
            msg <- paste("AWS :", stn.name, "\n", "Invalid date :", stn)
            writeUpload(msg, stn, "_format.txt")
            return(NULL)
        }
        daty <- daty[!ina]
        don <- don[!ina]

        # retv <- lapply(don, function(vdon){
        #     nomVars <- lapply(vdon, names)
        #     oldVars <<- merge.all.variables(oldVars, nomVars)
        #     return(0)
        # })

        mrgVars <- NULL
        for(iv in seq_along(don)){
            nomVars <- lapply(don[[iv]], names)
            mrgVars <- merge.all.variables(mrgVars, nomVars)
        }

        for(tt in  seq_along(daty)){
            out <- list(date = daty[tt], data = don[[tt]])
            file.out <- paste0(daty[tt], ".rds")
            file.loc <- file.path(dirL$dataLoc, file.out)
            file.up <- file.path(dirU$dataUp, file.out)
            saveRDS(out, file = file.loc)
            if(upload)
                ssh::scp_upload(session, file.loc, to = file.up, verbose = FALSE)
        }

        # return(daty)
        # })

        return(list(date = daty, nomvar = mrgVars))
    })

    inull <- sapply(retLoop, is.null)
    if(all(inull)) return(NULL)
    retLoop <- retLoop[!inull]

    daty <- lapply(retLoop, '[[', 'date')
    daty <- do.call(c, daty)
    daty <- daty[order(daty)]
    ndt <- length(daty)

    mrgVars <- lapply(retLoop, '[[', 'nomvar')
    for(jj in seq_along(mrgVars))
        oldVars <- merge.all.variables(oldVars, mrgVars[[jj]])

    # inull <- sapply(ret.don, is.null)
    # if(all(inull)) return(NULL)
    # ret.don <- ret.don[!inull]
    # daty <- do.call(c, ret.don)
    # daty <- daty[order(daty)]
    # ndt <- length(daty)

    if(!file.exists(dirL$info)){
        daty0 <- strptime(daty, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
        timestep <- as.numeric(names(which.max(table(diff(daty0)))))
        info <- list(id = stn.id, name = stn.name,
                     start = daty[1], end = daty[ndt],
                     vars = oldVars, tstep = timestep, updated = TRUE)
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
