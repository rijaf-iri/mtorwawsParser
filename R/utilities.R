
char_utc2local_time <- function(dates, format, tz = "Africa/Kigali"){
    x <- strptime(dates, format, tz = "UTC")
    x <- as.POSIXct(x)
    x <- format(x, format, tz = tz)
    x <- strptime(x, format, tz = tz)
    x
}

time_utc2local_char <- function(dates, format, tz = "Africa/Kigali"){
    x <- as.POSIXct(dates)
    x <- format(x, format, tz = tz)
    x
}

char_local2utc_time <- function(dates, format, tz = "Africa/Kigali"){
    x <- strptime(dates, format, tz = tz)
    x <- as.POSIXct(x)
    x <- format(x, format, tz = "UTC")
    x <- strptime(x, format, tz = "UTC")
    x
}

time_local2utc_char <- function(dates, format){
    x <- as.POSIXct(dates)
    x <- format(x, format, tz = "UTC")
    x
}

merge.all.variables <- function(oldVars, newVars){
    oldV <- names(oldVars)
    newV <- names(newVars)
    intV <- intersect(oldV, newV)
    if(length(intV) > 0){
        xnewV <- newV[!newV %in% intV]
        if(length(xnewV) > 0)
            oldVars <- c(oldVars, newVars[xnewV])

        for(v in intV){
           iv <- !newVars[[v]] %in% oldVars[[v]]
           if(any(iv))
                oldVars[[v]] <- c(oldVars[[v]], newVars[[v]][iv])
        }
    }else{
         oldVars <- c(oldVars, newVars)
    }

    return(oldVars)
}

split.date.by.month <- function(start_min, end_min, tz = "Africa/Kigali"){
    daty1 <- strptime(start_min, "%Y-%m-%d %H:%M", tz = tz)
    daty2 <- strptime(end_min, "%Y-%m-%d %H:%M", tz = tz)
    datys <- seq(daty1, daty2, 'day')
    daty_m <- format(datys, "%Y%m")
    daty_s <- lapply(split(seq_along(daty_m), daty_m), function(im){
        x <- datys[im]
        c(format(x[1], "%Y-%m-%d %H:%M"),
          format(x[length(x)], "%Y-%m-%d %H:%M"))
    })
    daty_s[[1]][1] <- paste(substr(daty_s[[1]][1], 1, 10), format(daty1, "%H:%M"))
    daty_s[[length(daty_s)]][2] <- paste(substr(daty_s[[length(daty_s)]][2], 1, 10), format(daty2, "%H:%M"))

    return(daty_s)
}

create_dirLoc_aws <- function(dirAWS, netAWS, stnID){
    dirInfoLoc <- file.path(dirAWS, "AWS_DATA", netAWS, "INFO")
    if(!dir.exists(dirInfoLoc))
        dir.create(dirInfoLoc, showWarnings = FALSE, recursive = TRUE)
    fileinfo <- file.path(dirInfoLoc, paste0(stnID, ".rds"))

    dirLog <- file.path(dirAWS, "AWS_DATA", netAWS, "LOG")

    dirLogLoc <- file.path(dirLog, "AWS", stnID)
    if(!dir.exists(dirLogLoc))
        dir.create(dirLogLoc, showWarnings = FALSE, recursive = TRUE)

    dirDataLoc <- file.path(dirAWS, "AWS_DATA", netAWS, "DATA", stnID)
    if(!dir.exists(dirDataLoc))
        dir.create(dirDataLoc, showWarnings = FALSE, recursive = TRUE)

    list(info = fileinfo, dirLog = dirLog,
         logLoc = dirLogLoc, dataLoc = dirDataLoc)
}

create_dirUp_aws <- function(session, dirUP, netAWS, stnID){
    dirInfoUp <- file.path(dirUP, netAWS, "INFO")
    ssh::ssh_exec_wait(session, command = c(
        paste0('if [ ! -d ', dirInfoUp, ' ] ; then mkdir -p ', dirInfoUp, ' ; fi')
    ))
    fileinfo <- file.path(dirInfoUp, paste0(stnID, ".rds"))

    dirLogUp <- file.path(dirUP, netAWS, "LOG", "AWS", stnID)
    ssh::ssh_exec_wait(session, command = c(
        paste0('if [ ! -d ', dirLogUp, ' ] ; then mkdir -p ', dirLogUp, ' ; fi')
    ))

    dirDataUp <- file.path(dirUP, netAWS, "DATA", stnID)
    ssh::ssh_exec_wait(session, command = c(
        paste0('if [ ! -d ', dirDataUp, ' ] ; then mkdir -p ', dirDataUp, ' ; fi')
    ))

    list(info = fileinfo, logUp = dirLogUp, dataUp = dirDataUp)
}

format.out.msg <- function(msg, logfile, append = TRUE){
    ret <- c(paste("Time:", Sys.time(), "\n"), msg, "\n",
             "*********************************\n")
    cat(ret, file = logfile, append = append)
}

read.rema.elab <- function(file, nrows = -1, skip = 0){
    utils::read.table(file, skip = skip,
                   nrows = nrows, sep = "\t",
                   colClasses = "character",
                   stringsAsFactors = FALSE,
                   row.names = NULL,
                   na.strings = "*")
}

difftime10Min <- function(x, y){
    df <- difftime(y, x, units = 'mins')
    as.numeric(df) / 10
}

list.depth <- function(l){
    ret <- 0
    if(is.list(l)){
        walk <- sapply(l, list.depth)
        ret <- 1 + max(walk)
    }

    ret
}

###################

doparallel.cond <- function(condition,
                            parll = list(dopar = TRUE,
                                         detect.cores = FALSE,
                                         nb.cores = 3)
                           )
{
    c(condition = condition, parll)
}

cdtdoparallel <- function(condition, dopar = TRUE,
                          detect.cores = TRUE, nb.cores = 2)
{
    okpar <- FALSE
    if(dopar){
        cores <- parallel::detectCores()
        if(detect.cores){
            nb.cores <- cores - 1
            okpar <- if(nb.cores >= 2) TRUE else FALSE
        }else{
            okpar <- if(cores >= 2 && nb.cores >= 2) TRUE else FALSE
        }
    }

    if(okpar & condition){
        klust <- parallel::makeCluster(nb.cores)
        doParallel::registerDoParallel(klust)
        `%dofun%` <- foreach::`%dopar%`
        closeklust <- TRUE
    }else{
        klust <- NULL
        `%dofun%` <- foreach::`%do%`
        closeklust <- FALSE
    }

    list(dofun = `%dofun%`, cluster = klust, parLL = closeklust)
}

utils::globalVariables(c('jloop'))

cdtforeach <- function(loopL, parsL, ..., FUN){
    FUN <- match.fun(FUN)
    if(missing(parsL)) parsL <- list(condition = FALSE)
    is.parallel <- do.call(cdtdoparallel, parsL)

    if(is.parallel$parLL){
        on.exit(parallel::stopCluster(is.parallel$cluster))
        `%parLoop%` <- is.parallel$dofun
        ret.loop <- foreach::foreach(jloop = loopL, ...) %parLoop% FUN(jloop)
    }else{
        ret.loop <- lapply(loopL, function(jloop){
            ret <- FUN(jloop)
            return(ret)
        })
    }

    return(ret.loop)
}
