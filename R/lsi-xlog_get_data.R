
get.lsi.xlog <- function(timeR, dirLSIXLOG, dirAWS, dirUP = NULL,
                         upload = TRUE, archive = FALSE)
{
    listAWS <- list.dirs(dirLSIXLOG, full.names = FALSE, recursive = FALSE)
    listAWS <- listAWS[listAWS != ""]
    liststn <- file.path(dirAWS, "AWS_PARAMS", "LSI_XLOG_list.txt")
    LSIXLOG <- utils::read.table(liststn, sep = ",", colClasses = "character", stringsAsFactors = FALSE)
    LSIXLOG <- apply(LSIXLOG, 2, function(x) trimws(as.character(x)))
    listAWS <- listAWS[listAWS %in% LSIXLOG[, 1]]
    if(length(listAWS) == 0) return(NULL)
    ix <- match(listAWS, LSIXLOG[, 1])
    ix <- ix[!is.na(ix)]
    listAWS <- LSIXLOG[ix, , drop = FALSE]
    dimnames(listAWS) <- NULL

    timeS <- format(timeR[1], "%Y%m%d%H")
    timeS <- strptime(timeS, "%Y%m%d%H", tz = "Africa/Kigali")
    timeList <- seq(timeS, timeR[2], "5 min")
    timeList <- format(timeList, "%Y%m%d%H%M")

    stnAWS <- lapply(seq(nrow(listAWS)), function(jj){
        aws_path <- file.path(dirLSIXLOG, listAWS[jj, 1])

        ## operational
        files_list <- paste0(listAWS[jj, 2], timeList, ".txt")
        ifile <- file.exists(file.path(aws_path, files_list))
        listFiles <- files_list[ifile]

        if(length(listFiles) == 0) return(NULL)
        daty <- gsub("\\.txt$", "", listFiles)
        daty <- trimws(substr(daty, 7, 18))

        daty <- strptime(daty, "%Y%m%d%H%M", tz = "Africa/Kigali")
        list(id = listAWS[jj, 2], name = listAWS[jj, 1], daty = daty, files = listFiles)
    })

    ###########

    inull <- sapply(stnAWS, is.null)
    if(any(inull))
        stnAWS <- stnAWS[!inull]

    ###########
    pars <- utils::read.table(file.path(dirAWS, "AWS_PARAMS", "LSI_XLOG_params.csv"),
                              header = TRUE, sep = ",", colClasses = "character", stringsAsFactors = FALSE)
    stats <- utils::read.table(file.path(dirAWS, "AWS_PARAMS", "LSI_XLOG_stats.csv"),
                               header = TRUE, sep = ",", colClasses = "character", stringsAsFactors = FALSE)
    pars <- pars[trimws(pars$Variable) != "", , drop = FALSE]
    stats <- stats[trimws(stats$STAT) != "", , drop = FALSE]
    params <- list(var = pars[, c("ID", "Variable")], stat = stats[, c("ID", "STAT")])
    params$var$ID <- as.numeric(params$var$ID)
    params$stat$ID <- as.numeric(params$stat$ID)

    ###########

    logAWS <- file.path(dirAWS, "AWS_DATA", "LSI-XLOG", "LOG", "AWS_LOG.txt")

    ret.aws <- lapply(seq_along(stnAWS), function(jstn){
        x <- stnAWS[[jstn]]
        if(is.null(x)) return(-1)
        out <- try(parse.lsi.xlog(x, params, dirLSIXLOG, dirAWS, dirUP, upload, archive), silent = TRUE)

        if(inherits(out, "try-error")){
            msg <- paste(out, "Unable to process :", x$name, x$id)
            format.out.msg(msg, logAWS)
            return(-1)
        }else{
            if(is.null(out)){
                msg <- paste("Some errors occurred, no update :", x$name, x$id)
                format.out.msg(msg, logAWS)
                return(1)
            }else{
                if(out == "no.update"){
                    msg <- paste("No new data, no update :", x$name, x$id)
                    format.out.msg(msg, logAWS)
                    return(0)
                }
                if(out == "updated"){
                    msg <- paste("Updated :", x$name, x$id)
                    format.out.msg(msg, logAWS)
                    return(0)
                }
            }
        }
    })

    return(0)
}
