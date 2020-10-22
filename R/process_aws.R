
Sys.setenv(TZ = "Africa/Kigali")

#' Process LSI-ELOG data.
#'
#' Get the data from Gidas database, parse and convert to a list object then upload it to "data-int" server.
#' 
#' @param dirAWS full path to the directory to store the parsed data.
#'               Example: "E:/MeteoRwanda"
#' @param dirUP full path to the directory to store the uploaded data in \code{data-int}.
#'              Default NULL, must be provided if \code{upload} is \code{TRUE}.
#'              Example: "/home/data/MeteoRwanda_Data/AWS_DATA/RAW"
#' @param upload logical, if TRUE the data will be uploaded to \code{data-int}
#' 
#' @export

process.lsi.elog <- function(dirAWS, dirUP = NULL, upload = TRUE)
{
    dirLOG <- file.path(dirAWS, "AWS_DATA", "LSI-ELOG", "LOG")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, "processing_log.txt")

    ## operational last 6 hours
    timeNow <- Sys.time()
    timeLast <- timeNow - 21600
    timeLast <- format(timeLast, "%Y-%m-%dT%H:%M:00")
    # discard invalid date
    timeNow <- format(timeNow + 60, "%Y-%m-%dT%H:%M:00")
    timeR <- c(timeLast, timeNow)

    ret <- try(get.lsi.elog(timeR, dirAWS, dirUP, upload,
               archive = FALSE), silent = TRUE)
    if(inherits(ret, "try-error")){ 
        msg <- paste(ret, "Getting LSI E-LOG data failed")
        format.out.msg(msg, logPROC)
    }
}

#' Process LSI-ELOG data archive mode.
#'
#' Get the data from Gidas database, parse and convert to a list object then upload it to "data-int" server.
#'
#' @param start_min the start time to process in the format "YYYY-MM-DD HH:MM".
#'                  Example: "2019-12-15 12:50"
#' @param end_min  the end time to process in the format "YYYY-MM-DD HH:MM"
#' @param dirAWS full path to the directory to store the parsed data.
#'               Example: "E:/MeteoRwanda"
#' @param dirUP full path to the directory to store the uploaded data in \code{data-int}.
#'              Default NULL, must be provided if \code{upload} is \code{TRUE}.
#'              Example: "/home/data/MeteoRwanda_Data/AWS_DATA/RAW"
#' @param upload logical, if TRUE the data will be uploaded to \code{data-int}
#' 
#' @export

process.lsi.elog_arch <- function(start_min, end_min, dirAWS, 
                                  dirUP = NULL, upload = TRUE)
{
    dirLOG <- file.path(dirAWS, "AWS_DATA", "LSI-ELOG", "LOG")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, "processing_log.txt")

    daty_s <- split.date.by.month(start_min, end_min, "Africa/Kigali")

    for(jj in seq_along(daty_s)){
        timeR <- strptime(daty_s[[jj]], "%Y-%m-%d %H:%M", tz = "Africa/Kigali")
        timeR <- format(timeR, "%Y-%m-%dT%H:%M:00")

        ret <- try(get.lsi.elog(timeR, dirAWS, dirUP, upload,
                   archive = TRUE), silent = TRUE)
        if(inherits(ret, "try-error")){ 
            msg <- paste(ret, "Getting LSI E-LOG data failed")
            format.out.msg(msg, logPROC)
        }
    }
}

#' Process LSI-XLOG data.
#'
#' Read the data from X-LOG storage, parse and convert to a list object then upload it to "data-int" server.
#' 
#' @param dirLSIXLOG full path to the directory of X-LOG.
#'                   Example: "C:/DATA/X-LOG"
#' @param dirAWS full path to the directory to store the parsed data.
#'               Example: "E:/MeteoRwanda"
#' @param dirUP full path to the directory to store the uploaded data in \code{data-int}.
#'              Default NULL, must be provided if \code{upload} is \code{TRUE}.
#'              Example: "/home/data/MeteoRwanda_Data/AWS_DATA/RAW"
#' @param upload logical, if TRUE the data will be uploaded to \code{data-int}
#' 
#' @export

process.lsi.xlog <- function(dirLSIXLOG, dirAWS, dirUP = NULL, upload = TRUE)
{
    dirLOG <- file.path(dirAWS, "AWS_DATA", "LSI-XLOG", "LOG")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, "processing_log.txt")

    ## operational last 6 hours
    timeNow <- Sys.time()
    timePast <- timeNow - 21600
    timeR <- c(timePast, timeNow)

    ret <- try(get.lsi.xlog(timeR, dirLSIXLOG, dirAWS, dirUP, upload,
               archive = FALSE), silent = TRUE)
    if(inherits(ret, "try-error")){ 
        msg <- paste(ret, "Getting LSI X-LOG data failed")
        format.out.msg(msg, logPROC)
    }
}

#' Process LSI-XLOG data archive mode.
#'
#' Read the data from X-LOG storage, parse and convert to a list object then upload it to "data-int" server.
#' 
#' @param start_min the start time to process in the format "YYYY-MM-DD HH:MM".
#'                  Example: "2019-12-15 12:50"
#' @param end_min  the end time to process in the format "YYYY-MM-DD HH:MM"
#' @param dirLSIXLOG full path to the directory of X-LOG.
#'                   Example: "C:/DATA/X-LOG"
#' @param dirAWS full path to the directory to store the parsed data.
#'               Example: "E:/MeteoRwanda"
#' @param dirUP full path to the directory to store the uploaded data in \code{data-int}.
#'              Default NULL, must be provided if \code{upload} is \code{TRUE}.
#'              Example: "/home/data/MeteoRwanda_Data/AWS_DATA/RAW"
#' @param upload logical, if TRUE the data will be uploaded to \code{data-int}
#' 
#' @export

process.lsi.xlog_arch <- function(start_min, end_min, dirLSIXLOG, dirAWS,
                                  dirUP = NULL, upload = TRUE)
{
    dirLOG <- file.path(dirAWS, "AWS_DATA", "LSI-XLOG", "LOG")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, "processing_log.txt")

    daty_s <- split.date.by.month(start_min, end_min, "Africa/Kigali")

    for(jj in seq_along(daty_s)){
        timeR <- strptime(daty_s[[jj]], "%Y-%m-%d %H:%M", tz = "Africa/Kigali")

        ret <- try(get.lsi.xlog(timeR, dirLSIXLOG, dirAWS, dirUP, upload,
                   archive = TRUE), silent = TRUE)
        if(inherits(ret, "try-error")){ 
            msg <- paste(ret, "Getting LSI X-LOG data failed")
            format.out.msg(msg, logPROC)
        }
    }
}

#' Process REMA data.
#'
#' Get the REMA data, parse and convert to a list object then upload it to "data-int" server.
#' 
#' @param dirELAB full path to the directory of the downloaded REMA elaborated data from FTP.
#'                   Example: "E:/MeteoRwanda/AWS_DATA/REMA/ELAB"
#' @param dirAWS full path to the directory to store the parsed data.
#'               Example: "E:/MeteoRwanda"
#' @param dirUP full path to the directory to store the uploaded data in \code{data-int}.
#'              Default NULL, must be provided if \code{upload} is \code{TRUE}.
#'              Example: "/home/data/MeteoRwanda_Data/AWS_DATA/RAW"
#' @param upload logical, if TRUE the data will be uploaded to \code{data-int}
#' 
#' @export

process.rema <- function(dirELAB, dirAWS, dirUP = NULL, upload = TRUE)
{
    dirLOG <- file.path(dirAWS, "AWS_DATA", "REMA", "LOG")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, "processing_log.txt")

    ## operational last 6 hours
    timeNow <- Sys.time()
    timePast <- timeNow - 21600
    timeR <- c(timePast, timeNow)

    ret <- try(get.rema(timeR, dirELAB, dirAWS, dirUP, upload,
               archive = FALSE), silent = TRUE)
    if(inherits(ret, "try-error")){
        msg <- paste(ret, "Getting REMA data failed")
        format.out.msg(msg, logPROC)
    }
}

#' Process REMA data archive mode.
#'
#' Get the REMA data, parse and convert to a list object then upload it to "data-int" server.
#' 
#' @param start_min the start time to process in the format "YYYY-MM-DD HH:MM".
#'                  Example: "2019-12-15 12:50"
#' @param end_min  the end time to process in the format "YYYY-MM-DD HH:MM"
#' @param dirELAB full path to the directory of the downloaded REMA elaborated data from FTP.
#'                   Example: "E:/MeteoRwanda/AWS_DATA/REMA/ELAB"
#' @param dirAWS full path to the directory to store the parsed data.
#'               Example: "E:/MeteoRwanda"
#' @param dirUP full path to the directory to store the uploaded data in \code{data-int}.
#'              Default NULL, must be provided if \code{upload} is \code{TRUE}.
#'              Example: "/home/data/MeteoRwanda_Data/AWS_DATA/RAW"
#' @param upload logical, if TRUE the data will be uploaded to \code{data-int}
#' 
#' @export

process.rema_arch <- function(start_min, end_min, dirELAB, dirAWS,
                              dirUP = NULL, upload = TRUE)
{
    dirLOG <- file.path(dirAWS, "AWS_DATA", "REMA", "LOG")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, "processing_log.txt")

    time1 <- strptime(start_min, "%Y-%m-%d %H:%M", tz = "Africa/Kigali")
    time2 <- strptime(end_min, "%Y-%m-%d %H:%M", tz = "Africa/Kigali")
    timeR <- c(time1, time2)

    ret <- try(get.rema(timeR, dirELAB, dirAWS, dirUP, upload,
               archive = TRUE), silent = TRUE)
    if(inherits(ret, "try-error")){
        msg <- paste(ret, "Getting REMA data failed")
        format.out.msg(msg, logPROC)
    }
}

#' Download REMA data from the FTP server.
#'
#' Download REMA data from the FTP server.
#' 
#' @param dirAWS full path to the directory to store the parsed data.
#'               Example: "E:/MeteoRwanda"
#' @details
#' The downloaded data are stored under "E:/MeteoRwanda/AWS_DATA/REMA/ELAB" if \code{dirAWS} is "E:/MeteoRwanda"
#' 
#' @export

download.rema <- function(dirAWS){
    dirREMA <- file.path(dirAWS, "AWS_DATA", "REMA")
    dirLOG <- file.path(dirREMA, "LOG")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logFTP <- file.path(dirLOG, "FTP_REMA_LOG.txt")
    dirELAB <- file.path(dirREMA, "ELAB")
    if(!dir.exists(dirELAB))
        dir.create(dirELAB, showWarnings = FALSE, recursive = TRUE)
    ftp <- readRDS(file.path(dirAWS, "AWS_PARAMS", "ftp-rema.cred"))

    handle <- curl::new_handle()
    ftp$cred$handle <- handle
    session <- try(do.call(curl::handle_setopt, ftp$cred), silent = TRUE)

    lnk <- paste0(ftp$url, paste0("gateway_data_", 301:326, ".txt"))
    dest <- file.path(dirELAB, paste0("gateway_data_", 301:326, ".txt"))

    for(j in seq_along(lnk)){
        dc <- try(curl::curl_download(lnk[j], dest[j], handle = handle), silent = TRUE)
        if(inherits(dc, "try-error")){ 
            msg <- paste(dc, "Unable to download:", basename(dest[j]))
            format.out.msg(msg, logFTP)
        }
    }
}

#' Upload log files to "data-int" server.
#'
#' Upload log files to "data-int" server.
#' 
#' @param dirAWS full path to the directory to store the parsed data.
#'               Example: "E:/MeteoRwanda"
#' @param dirUP full path to the directory to store the uploaded data in \code{data-int}.
#' 
#' @export

upload.logFiles <- function(dirAWS, dirUP){
    on.exit(ssh::ssh_disconnect(session))

    ssh <- readRDS(file.path(dirAWS, "AWS_PARAMS", "data-int.cred"))
    session <- try(do.call(ssh::ssh_connect, ssh$cred), silent = TRUE)
    if(inherits(session, "try-error")) return(NULL)

    netAWS <- c('REMA', 'LSI-XLOG', 'LSI-ELOG')
    for (aws in netAWS){
        dirLOC <- file.path(dirAWS, "AWS_DATA", aws, "LOG")
        dirREM <- file.path(dirUP, aws, "LOG")

        filesUp <- c("processing_log.txt", "UPLOAD_LOG.txt", "AWS_LOG.txt")
        for(ff in filesUp){
            f1 <- file.path(dirLOC, ff)
            f2 <- file.path(dirREM, ff)
            if(file.exists(f1))
                ssh::scp_upload(session, f1, to = f2, verbose = FALSE)
        }
    }
}


