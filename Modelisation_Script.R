
library(RODBC)
library(nnet)
library(ROSE)
library(caret)
library(glmnet)

Mape = function(y_actual, y_predict){
  res = mean(abs((y_actual-y_predict)/y_actual))*100
  return(res)
}

Rsquare = function(y_actual,y_predict){
  res = cor(y_actual,y_predict)^2
  return(res)
}



# Connect to MySQL (my credentials are mysql_server_64/root/.)
db = odbcConnect("mysql_server_64", uid="root", pwd="NicoSQL00!")
sqlQuery(db, "USE ma_charity")


# LOGIT

query = 'WITH sol_sj AS (SELECT ass.contact_id, a.campaign_id, COUNT(message_id) as sol_sent
                    			FROM assignment2 ass LEFT JOIN actions a ON (ass.contact_id=a.contact_id) 
                                WHERE (YEAR(a.action_date)>2017) AND (LEFT(a.campaign_id, 2) LIKE "SJ") AND (ass.calibration=1)
                                GROUP BY 1, 2 ORDER BY 1),
            	sol_s1 AS (SELECT ass.contact_id, a.campaign_id, COUNT(message_id) as sol_sent
                    			FROM assignment2 ass LEFT JOIN actions a ON (ass.contact_id=a.contact_id) 
                                WHERE (YEAR(a.action_date)>2017) AND (LEFT(a.campaign_id, 2) IN ("SF", "SV")) AND (ass.calibration=1)
                                GROUP BY 1, 2 ORDER BY 1),
            	sol_s2 AS (SELECT ass.contact_id, a.campaign_id, COUNT(message_id) as sol_sent
                    			FROM assignment2 ass LEFT JOIN actions a ON (ass.contact_id=a.contact_id) 
                                WHERE (YEAR(a.action_date)>2017) AND (LEFT(a.campaign_id, 2) IN ("SA", "SO")) AND (ass.calibration=1)
                                GROUP BY 1, 2 ORDER BY 1),
            	don AS (SELECT ass.contact_id, ac.campaign_id, COUNT(ac.amount) as don_rec
                    			FROM assignment2 ass LEFT JOIN acts ac ON (ass.contact_id=ac.contact_id)
                                WHERE ac.act_type_id="DO"
                                GROUP BY 1, 2 ORDER BY 1),
            	resp_sj AS (SELECT sol_sj.contact_id, 
                    				CASE WHEN SUM(don.don_rec) IS NULL THEN 0 ELSE SUM(don.don_rec)/SUM(sol_sj.sol_sent) END AS response_sj
                    				FROM sol_sj LEFT JOIN don ON (sol_sj.contact_id=don.contact_id) AND (sol_sj.campaign_id=don.campaign_id)
                    				GROUP BY sol_sj.contact_id
                    				ORDER BY 1),
            	resp_s1 AS (SELECT sol_s1.contact_id, 
                    				CASE WHEN SUM(don.don_rec) IS NULL THEN 0 ELSE SUM(don.don_rec)/SUM(sol_s1.sol_sent) END AS response_s1
                    				FROM sol_s1 LEFT JOIN don ON (sol_s1.contact_id=don.contact_id) AND (sol_s1.campaign_id=don.campaign_id)
                    				GROUP BY sol_s1.contact_id
                    				ORDER BY 1),
              	resp_s2 AS (SELECT sol_s2.contact_id, 
                    				CASE WHEN SUM(don.don_rec) IS NULL THEN 0 ELSE SUM(don.don_rec)/SUM(sol_s2.sol_sent) END AS response_s2
                    				FROM sol_s2 LEFT JOIN don ON (sol_s2.contact_id=don.contact_id) AND (sol_s2.campaign_id=don.campaign_id)
                    				GROUP BY sol_s2.contact_id
                    				ORDER BY 1),
              	res_1 AS (SELECT resp_sj.*, CASE WHEN resp_s1.response_s1 IS NULL THEN 0 ELSE resp_s1.response_s1 END as resp_s1
                    				FROM resp_sj LEFT JOIN resp_s1 ON (resp_sj.contact_id=resp_s1.contact_id)),
              	res_2 AS (SELECT resp_sj.*, CASE WHEN resp_s2.response_s2 IS NULL THEN 0 ELSE resp_s2.response_s2 END as resp_s2
                    				FROM resp_sj LEFT JOIN resp_s2 ON (resp_sj.contact_id=resp_s2.contact_id)),
              	res AS   (SELECT res_1.*, res_2.resp_s2
                    				FROM res_1 LEFT JOIN res_2 ON (res_1.contact_id=res_2.contact_id)),
                contact_info AS (SELECT ass.contact_id as id, ass.donation AS target_p, 
                    					IF (c.prefix_id IN ("ASSO", "SOC"), 1, 0) AS pref_group,
                    					IF (c.prefix_id IN ("MLLE", "MME"), 1, 0) AS pref_fem,
                    					IF (c.prefix_id IN ("DR", "MR", "ME", "MMME"), 1, 0) AS pref_male
                    					FROM assignment2 ass JOIN contacts c ON (ass.contact_id = c.id)
                    					WHERE (ass.calibration=1) AND (c.active=1)
                    					ORDER BY 1),
              	act_info AS (SELECT  ass.contact_id,
                    					COUNT(ac.amount) AS frequency,
                    					DATEDIFF(20220530, MAX(ac.act_date)) / 365 AS recency,
                    					DATEDIFF(20220530, MIN(ac.act_date)) / 365 AS first_donation
                    					FROM assignment2 ass JOIN acts ac ON (ass.contact_id = ac.contact_id)
                    					WHERE (ass.calibration=1) AND (ac.act_type_id="DO")
                    					GROUP BY 1 ORDER BY 1)
                SELECT q.*, a.frequency, a.recency, a.first_donation
                FROM 
                (SELECT c.*, r.response_sj, r.resp_s1, r.resp_s2 FROM contact_info c JOIN res r ON (c.id=r.contact_id)) q 
                JOIN act_info a ON (q.id=a.contact_id) 
                ORDER BY 1;'

log_data_train = sqlQuery(db, query, max = 0) 
# 38803 observations are extracted as intended: 38803 is the number of donors
# at the intersection between those that are active and those that made at least one one-off donations

rownames(log_data_train) = log_data_train$id
log_data_train = log_data_train[, -1]

log_data_train$response_sj.sq = log_data_train$response_sj^2
log_data_train$resp_s1.sq = log_data_train$resp_s1^2


# formula is the result of many attempts, the Logit seemed to be robust to 
# correlation between variables (checked with CV for the generalization), 
# so interaction and square terms are inserted and No regularization technique
# is implemented. The additional features are the prefix groups, and response
# rates (n. donations/n. sollicitations sent) for the SJ campaign, as well as the
# SF-SV campaigns (first part of the year), and SA-SO (second part)

form = paste("target_p ~ (recency * frequency) + log(recency) + log(frequency)",
             "+ first_donation + pref_fem + pref_group + response_sj + resp_s1 + resp_s2",
             "+ response_sj:resp_s1 + response_sj:resp_s2 + resp_s1:resp_s2 + resp_s1.sq")  # + pref_male + pref_couple + 

prob.model = multinom(formula=form, data=log_data_train) 


# REGRESSION
# two separate queries are conducted for the 2 different problems (probability and amount)
# but attention is put to ensure the number of users at the intersection of the necessary
# information (38803) is the same
# the new features is the average amount donated in SJ (0 if never donated)

query = 'WITH orig AS (SELECT ass.contact_id, 
              COUNT(ac.amount) AS frequency,
              MAX(ac.amount) AS max_amount,
              MIN(ac.amount) AS min_amount,
              AVG(ac.amount) AS avg_amount
              FROM assignment2 ass JOIN acts ac ON (ass.contact_id = ac.contact_id)
              WHERE (ass.calibration=1) AND (ac.act_date>2017) AND (ac.act_type_id="DO")
              GROUP BY 1 ORDER BY 1),
          contact_info AS (SELECT ass.contact_id as id, ass.amount AS target_reg, 
                           IF (c.prefix_id IN ("ASSO", "SOC"), 1, 0) AS pref_group,
                           IF (c.prefix_id IN ("MLLE", "MME"), 1, 0) AS pref_fem,
                           IF (c.prefix_id IN ("DR", "MR", "ME", "MMME"), 1, 0) AS pref_male,
                           IF (c.zip_code div 1000 in (1, 3, 7, 15, 26, 38, 42, 43, 63, 69, 73, 74, 75, 77, 78, 91, 92, 93, 94, 95), 1, 0) AS region_1,
                           IF (c.zip_code div 1000 in (8, 14, 18, 21, 22, 25, 27, 29, 35, 36, 37, 39, 41, 45, 50, 56, 58, 61, 70, 71, 76, 89, 90, 96), 1, 0) AS region_2
                           FROM assignment2 ass JOIN contacts c ON (ass.contact_id = c.id)
                           WHERE (ass.calibration=1) AND (c.active=1)
                           ORDER BY 1),
            don_info_sj AS (SELECT ass.contact_id, COUNT(ac.amount) as don_rec, SUM(ac.amount) as tot_rec
                         FROM assignment2 ass LEFT JOIN acts ac ON (ass.contact_id=ac.contact_id)
                         WHERE (ac.act_type_id="DO") AND (YEAR(ac.act_date)>2018) AND (LEFT(ac.campaign_id, 2) LIKE "SJ") AND (ass.calibration=1)
                         GROUP BY 1 ORDER BY 1),
      			don_info_s1 AS (SELECT ass.contact_id, COUNT(ac.amount) as don_rec, SUM(ac.amount) as tot_rec
                               FROM assignment2 ass LEFT JOIN acts ac ON (ass.contact_id=ac.contact_id)
                               WHERE (ac.act_type_id="DO") AND (YEAR(ac.act_date)>2018) AND (LEFT(ac.campaign_id, 2) IN ("SF", "SV")) AND (ass.calibration=1)
                               GROUP BY 1 ORDER BY 1),
      			don_info_s2 AS (SELECT ass.contact_id, COUNT(ac.amount) as don_rec, SUM(ac.amount) as tot_rec
                               FROM assignment2 ass LEFT JOIN acts ac ON (ass.contact_id=ac.contact_id)
                               WHERE (ac.act_type_id="DO") AND (YEAR(ac.act_date)>2018) AND (LEFT(ac.campaign_id, 2) IN ("SA", "SO")) AND (ass.calibration=1)
                               GROUP BY 1 ORDER BY 1)
                         
          SELECT c.id, c.target_reg, o.frequency, o.max_amount, o.min_amount, o.avg_amount, 
					c.pref_group, c.pref_fem, c.pref_male, c.region_1, c.region_2, 
                    CASE WHEN dj.tot_rec IS NULL THEN 0 ELSE dj.tot_rec/dj.don_rec END AS average_sj,
                    CASE WHEN d1.tot_rec IS NULL THEN 0 ELSE d1.tot_rec/d1.don_rec END AS average_s1,
                    CASE WHEN d2.tot_rec IS NULL THEN 0 ELSE d2.tot_rec/d2.don_rec END AS average_s2
                  FROM contact_info c JOIN orig o ON (o.contact_id=c.id) 
                  LEFT JOIN don_info_sj dj ON (c.id=dj.contact_id) 
                  LEFT JOIN don_info_s1 d1 ON (c.id=d1.contact_id) 
                  LEFT JOIN don_info_s2 d2 ON (c.id=d2.contact_id) ORDER BY 1;'

reg_data_train = sqlQuery(db, query, max = 0) 
rownames(reg_data_train) = reg_data_train$id
reg_data_train = reg_data_train[, -1]

# formula chosen after many attempts
# for Regression we have few observations, so that it is prone to overfitting
# few variables are inserted (chosen with trial-and-error for the scores on 
# the calibration data) and on top of that Ridge regularization is performed
# (it was seen that all the measures of accuracy used, which are not reported here)
# were more stable with Cross Validation.

form = paste("log(target_reg) ~ average_sj*log(avg_amount) + log(max_amount) + min_amount")

idx = which(!is.na(reg_data_train$target_reg))  
sub_log_reg <- reg_data_train[idx,]   # submatrix of observations for which amount is not null

x_train = cbind(sub_log_reg$average_sj, log(sub_log_reg$avg_amount), sub_log_reg$average_sj * log(sub_log_reg$avg_amount),
          log(sub_log_reg$max_amount), sub_log_reg$min_amount)

y_train = log(sub_log_reg$target_reg)  

lambdas <- seq(10^1, 10^(-3), len=150)  # grid for penalization parameter
cv.ridge = cv.glmnet(x_train, y_train, alpha = 0, family="gaussian", nfolds=10, lambda=lambdas)   # Ridge regularization
opt_lambda <- cv.ridge$lambda.min
print(paste("The optimal lambda for Ridge is: ", opt_lambda))


ridge.mod = glmnet(x_train, y_train, alpha = 0, family = 'gaussian', lambda = opt_lambda)


# ---------------------------------------------------------------------------------------
# TEST DATA
# ---------------------------------------------------------------------------------------


query = "SELECT contact_id FROM assignment2 ass WHERE ass.calibration=0 ORDER BY contact_id"

unique_id = sqlQuery(db, query, max = 0)

out = data.frame(id = unique_id$contact_id)  # store with sorted contact_id in Test group
rownames(out) = out$id
out$probs = 0
out$amount = 0

# LOGIT

query='WITH sol_sj AS (SELECT ass.contact_id, a.campaign_id, COUNT(message_id) as sol_sent
                    			FROM assignment2 ass LEFT JOIN actions a ON (ass.contact_id=a.contact_id) 
                                WHERE (YEAR(a.action_date)>2017) AND (LEFT(a.campaign_id, 2) LIKE "SJ") AND (ass.calibration=0)
                                GROUP BY 1, 2 ORDER BY 1),
            	sol_s1 AS (SELECT ass.contact_id, a.campaign_id, COUNT(message_id) as sol_sent
                    			FROM assignment2 ass LEFT JOIN actions a ON (ass.contact_id=a.contact_id) 
                                WHERE (YEAR(a.action_date)>2017) AND (LEFT(a.campaign_id, 2) IN ("SF", "SV")) AND (ass.calibration=0)
                                GROUP BY 1, 2 ORDER BY 1),
            	sol_s2 AS (SELECT ass.contact_id, a.campaign_id, COUNT(message_id) as sol_sent
                    			FROM assignment2 ass LEFT JOIN actions a ON (ass.contact_id=a.contact_id) 
                                WHERE (YEAR(a.action_date)>2017) AND (LEFT(a.campaign_id, 2) IN ("SA", "SO")) AND (ass.calibration=0)
                                GROUP BY 1, 2 ORDER BY 1),
            	don AS (SELECT ass.contact_id, ac.campaign_id, COUNT(ac.amount) as don_rec
                    			FROM assignment2 ass LEFT JOIN acts ac ON (ass.contact_id=ac.contact_id)
                                WHERE ac.act_type_id="DO"
                                GROUP BY 1, 2 ORDER BY 1),
            	resp_sj AS (SELECT sol_sj.contact_id, 
                    				CASE WHEN SUM(don.don_rec) IS NULL THEN 0 ELSE SUM(don.don_rec)/SUM(sol_sj.sol_sent) END AS response_sj
                    				FROM sol_sj LEFT JOIN don ON (sol_sj.contact_id=don.contact_id) AND (sol_sj.campaign_id=don.campaign_id)
                    				GROUP BY sol_sj.contact_id
                    				ORDER BY 1),
            	resp_s1 AS (SELECT sol_s1.contact_id, 
                    				CASE WHEN SUM(don.don_rec) IS NULL THEN 0 ELSE SUM(don.don_rec)/SUM(sol_s1.sol_sent) END AS response_s1
                    				FROM sol_s1 LEFT JOIN don ON (sol_s1.contact_id=don.contact_id) AND (sol_s1.campaign_id=don.campaign_id)
                    				GROUP BY sol_s1.contact_id
                    				ORDER BY 1),
              	resp_s2 AS (SELECT sol_s2.contact_id, 
                    				CASE WHEN SUM(don.don_rec) IS NULL THEN 0 ELSE SUM(don.don_rec)/SUM(sol_s2.sol_sent) END AS response_s2
                    				FROM sol_s2 LEFT JOIN don ON (sol_s2.contact_id=don.contact_id) AND (sol_s2.campaign_id=don.campaign_id)
                    				GROUP BY sol_s2.contact_id
                    				ORDER BY 1),
              	res_1 AS (SELECT resp_sj.*, CASE WHEN resp_s1.response_s1 IS NULL THEN 0 ELSE resp_s1.response_s1 END as resp_s1
                    				FROM resp_sj LEFT JOIN resp_s1 ON (resp_sj.contact_id=resp_s1.contact_id)),
              	res_2 AS (SELECT resp_sj.*, CASE WHEN resp_s2.response_s2 IS NULL THEN 0 ELSE resp_s2.response_s2 END as resp_s2
                    				FROM resp_sj LEFT JOIN resp_s2 ON (resp_sj.contact_id=resp_s2.contact_id)),
              	res AS   (SELECT res_1.*, res_2.resp_s2
                    				FROM res_1 LEFT JOIN res_2 ON (res_1.contact_id=res_2.contact_id)),
                contact_info AS (SELECT ass.contact_id as id,
                    					IF (c.prefix_id IN ("ASSO", "SOC"), 1, 0) AS pref_group,
                    					IF (c.prefix_id IN ("MLLE", "MME"), 1, 0) AS pref_fem,
                    					IF (c.prefix_id IN ("DR", "MR", "ME", "MMME"), 1, 0) AS pref_male
                    					FROM assignment2 ass JOIN contacts c ON (ass.contact_id = c.id)
                    					WHERE (ass.calibration=0) AND (c.active=1)
                    					ORDER BY 1),
              	act_info AS (SELECT  ass.contact_id,
                    					COUNT(ac.amount) AS frequency,
                    					DATEDIFF(20220530, MAX(ac.act_date)) / 365 AS recency,
                    					DATEDIFF(20220530, MIN(ac.act_date)) / 365 AS first_donation
                    					FROM assignment2 ass JOIN acts ac ON (ass.contact_id = ac.contact_id)
                    					WHERE (ass.calibration=0) AND (ac.act_type_id="DO")
                    					GROUP BY 1 ORDER BY 1)
                SELECT q.*, a.frequency, a.recency, a.first_donation
                FROM 
                (SELECT c.*, r.response_sj, r.resp_s1, r.resp_s2 FROM contact_info c JOIN res r ON (c.id=r.contact_id)) q 
                JOIN act_info a ON (q.id=a.contact_id) 
                ORDER BY 1;'

log_data_test = sqlQuery(db, query, max = 0) 

rownames(log_data_test) = log_data_test$id
log_data_test = log_data_test[, -1]

log_data_test$response_sj.sq = log_data_test$response_sj^2  # feature engineering as before necessary for the formula
log_data_test$resp_s1.sq = log_data_test$resp_s1^2

log_data_test$predicted = predict(object = prob.model, newdata = log_data_test, type = "probs")
out[rownames(log_data_test), 2] = log_data_test$predicted  


# REGRESSION

query = 'WITH orig AS (SELECT ass.contact_id, 
              COUNT(ac.amount) AS frequency,
              MAX(ac.amount) AS max_amount,
              MIN(ac.amount) AS min_amount,
              AVG(ac.amount) AS avg_amount
              FROM assignment2 ass JOIN acts ac ON (ass.contact_id = ac.contact_id)
              WHERE (ass.calibration=0) AND (ac.act_date>2017) AND (ac.act_type_id="DO")
              GROUP BY 1 ORDER BY 1),
          contact_info AS (SELECT ass.contact_id as id,
                           IF (c.prefix_id IN ("ASSO", "SOC"), 1, 0) AS pref_group,
                           IF (c.prefix_id IN ("MLLE", "MME"), 1, 0) AS pref_fem,
                           IF (c.prefix_id IN ("DR", "MR", "ME", "MMME"), 1, 0) AS pref_male,
                           IF (c.zip_code div 1000 in (1, 3, 7, 15, 26, 38, 42, 43, 63, 69, 73, 74, 75, 77, 78, 91, 92, 93, 94, 95), 1, 0) AS region_1,
                           IF (c.zip_code div 1000 in (8, 14, 18, 21, 22, 25, 27, 29, 35, 36, 37, 39, 41, 45, 50, 56, 58, 61, 70, 71, 76, 89, 90, 96), 1, 0) AS region_2
                           FROM assignment2 ass JOIN contacts c ON (ass.contact_id = c.id)
                           WHERE (ass.calibration=0) AND (c.active=1)
                           ORDER BY 1),
            don_info_sj AS (SELECT ass.contact_id, COUNT(ac.amount) as don_rec, SUM(ac.amount) as tot_rec
                         FROM assignment2 ass LEFT JOIN acts ac ON (ass.contact_id=ac.contact_id)
                         WHERE (ac.act_type_id="DO") AND (YEAR(ac.act_date)>2018) AND (LEFT(ac.campaign_id, 2) LIKE "SJ") AND (ass.calibration=0)
                         GROUP BY 1 ORDER BY 1),
      			don_info_s1 AS (SELECT ass.contact_id, COUNT(ac.amount) as don_rec, SUM(ac.amount) as tot_rec
                               FROM assignment2 ass LEFT JOIN acts ac ON (ass.contact_id=ac.contact_id)
                               WHERE (ac.act_type_id="DO") AND (YEAR(ac.act_date)>2018) AND (LEFT(ac.campaign_id, 2) IN ("SF", "SV")) AND (ass.calibration=0)
                               GROUP BY 1 ORDER BY 1),
      			don_info_s2 AS (SELECT ass.contact_id, COUNT(ac.amount) as don_rec, SUM(ac.amount) as tot_rec
                               FROM assignment2 ass LEFT JOIN acts ac ON (ass.contact_id=ac.contact_id)
                               WHERE (ac.act_type_id="DO") AND (YEAR(ac.act_date)>2018) AND (LEFT(ac.campaign_id, 2) IN ("SA", "SO")) AND (ass.calibration=0)
                               GROUP BY 1 ORDER BY 1)
                         
          SELECT c.id, o.frequency, o.max_amount, o.min_amount, o.avg_amount, 
					c.pref_group, c.pref_fem, c.pref_male, c.region_1, c.region_2, 
                    CASE WHEN dj.tot_rec IS NULL THEN 0 ELSE dj.tot_rec/dj.don_rec END AS average_sj,
                    CASE WHEN d1.tot_rec IS NULL THEN 0 ELSE d1.tot_rec/d1.don_rec END AS average_s1,
                    CASE WHEN d2.tot_rec IS NULL THEN 0 ELSE d2.tot_rec/d2.don_rec END AS average_s2
                  FROM contact_info c JOIN orig o ON (o.contact_id=c.id) 
                  LEFT JOIN don_info_sj dj ON (c.id=dj.contact_id) 
                  LEFT JOIN don_info_s1 d1 ON (c.id=d1.contact_id) 
                  LEFT JOIN don_info_s2 d2 ON (c.id=d2.contact_id) ORDER BY 1;'

reg_data_test = sqlQuery(db, query, max = 0) 

rownames(reg_data_test) = reg_data_test$id
reg_data_test = reg_data_test[, -1]


x_test = cbind(reg_data_test$average_sj, log(reg_data_test$avg_amount), reg_data_test$average_sj * log(reg_data_test$avg_amount),
                log(reg_data_test$max_amount), reg_data_test$min_amount)


reg_data_test$predicted = exp(predict(ridge.mod, s = opt_lambda, newx = x_test))


out[rownames(reg_data_test), 3] = reg_data_test$predicted


#--------------------------------

out$score  = out$probs * out$amount
out$solicit = ifelse(out$score>2, 1, 0)   # solicit


# we make sure NOT TO SOLICIT particular groups of contacts

# Those for which we have no act info
query = 'WITH absent as (SELECT ass.contact_id 
				FROM assignment2 ass 
				LEFT JOIN acts a ON (ass.contact_id = a.contact_id)
				WHERE (ass.calibration=0) and (a.act_date IS NULL)
				UNION
				SELECT ass.contact_id FROM assignment2 ass 
				LEFT JOIN contacts c ON (ass.contact_id = c.id)
				WHERE (ass.calibration=0) and (c.prefix_id IS NULL))
				
        SELECT a.contact_id as id from assignment2 a INNER JOIN absent ab 
        ON (a.contact_id=ab.contact_id) ORDER BY 1;'

no_info = sqlQuery(db, query, max = 0) 
rownames(no_info) = no_info$id

out[rownames(no_info), 5] = 0



# Inactive
query='SELECT q.contact_id as id
        FROM
        (SELECT * FROM assignment2 ass 
          INNER JOIN contacts c ON (ass.contact_id = c.id)) AS q
        INNER JOIN acts a
        ON (q.contact_id = a.contact_id)
        WHERE (q.calibration=0) AND (active=0) GROUP BY 1 ORDER BY 1;'

inactive = sqlQuery(db, query, max = 0) 
rownames(inactive) = inactive$id

out[rownames(inactive), 5] = 0


final = data.frame(id = out$id, solicit=out$solicit)

write.table(final, file = "FINAL.txt", sep = "\t",
            row.names = FALSE, col.names=FALSE)










# ---------------------------------------------------------------
# this was to exclude the contacts for which the last 10 donations
# were automatic donations 
# eventually I did not use it

query= 'WITH frame AS (SELECT ass.contact_id, 
                       a.act_date, 
                       a.act_type_id, 
                       ROW_NUMBER() OVER (PARTITION BY ass.contact_id ORDER BY a.act_date DESC) AS row_nb
                       FROM assignment2 ass JOIN acts a ON (ass.contact_id=a.contact_id) ORDER BY 1, 4 ASC),
        frame2 AS (SELECT frame.*, 
                   CASE WHEN ((frame.act_type_id="PA") AND (frame.row_nb < 11)) THEN 1 ELSE 0 END AS pa_recent
                   FROM frame),
        final AS (SELECT contact_id, SUM(pa_recent) AS fin
                  FROM frame2
                  GROUP BY 1)
        SELECT contact_id FROM final WHERE fin=10 ORDER BY final.contact_id;'
autom_deduction = sqlQuery(db, query, max = 0) 
rownames(autom_deduction) = autom_deduction$contact_id

autom_deduction = autom_deduction[order(rownames(autom_deduction)), ]
  
  
  
  
  









