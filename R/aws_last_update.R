
#' Process LSI-ELOG data.
#'
#' Get the data from Gidas database, parse and convert to a list object then upload it to "data-int" server.
#' 
#' @param dirAWS full path to the directory to store the parsed data.
#'               Example: "E:/MeteoRwanda_v2/AWS_DATA"
#' 
#' @return a data.frame
#' 
#' @export

check_aws_last_update <- function(dirAWS){
    info <- lapply(c("LSI-ELOG", "LSI-XLOG", "REMA"), function(awsnet){
        infopath <- file.path(dirAWS, awsnet, "INFO")
        infords <- list.files(infopath, "\\.rds$")
        infoend <- lapply(infords, function(x){
            info <- readRDS(file.path(infopath, x))
            do.call(c, info[c('id', 'end')])
        })
        infoend <- do.call(rbind, infoend)
        cbind(awsnet, infoend)
    })

    info <- do.call(rbind, info)
    info <- info[order(info[, 3]), ]
    info <- as.data.frame(info)
    names(info) <- c("Network", "ID", "Last_Data")
    tt <- strptime(info$Last_Data, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
    info$Last_Data <- format(tt, "%Y-%m-%d %H:%M")
    info
}

