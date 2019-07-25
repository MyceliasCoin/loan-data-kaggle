# create empty loans table in PostgreSQL with appropriate datatypes and populate from loans.csv file
import config
from sqlalchemy import create_engine


def create_db_engine():
    """
    Connects to PostgreSQL instance
    :return: PostgreSQL engine object
    """
    engine = create_engine(config.PG_CONFIG['PG_FORMAT'] + '://' + config.PG_CONFIG['PG_USER'] + ':' + config.PG_CONFIG['PG_PASSWORD'] + '@' + config.PG_CONFIG['PG_URL'] + ':' + config.PG_CONFIG['PG_PORT'] + '/' + config.PG_CONFIG['PG_DB'])
    return engine


def create_loans_table(pg_engine):
    """
    Creates empty loans table in PostgreSQL
    :param pg_engine: PostgreSQL engine object
    :return: True
    """
    create_query = "CREATE TABLE IF NOT EXISTS loans (id VARCHAR, member_id VARCHAR, loan_amnt FLOAT (2), funded_amnt FLOAT (2), funded_amnt_inv FLOAT (2), term VARCHAR (20), int_rate FLOAT (2), installment FLOAT (2), grade VARCHAR (1), sub_grade VARCHAR (2), emp_title VARCHAR, emp_length VARCHAR (10), home_ownership VARCHAR (10), annual_inc FLOAT (2), verification_status VARCHAR (20), issue_d VARCHAR (10), loan_status VARCHAR, pymnt_plan VARCHAR (1), url VARCHAR, \"desc\" VARCHAR, purpose VARCHAR, title VARCHAR, zip_code VARCHAR (10), addr_state VARCHAR (2), dti FLOAT (2), delinq_2yrs SMALLINT, earliest_cr_line VARCHAR (10), inq_last_6mths SMALLINT, mths_since_last_delinq SMALLINT, mths_since_last_record SMALLINT, open_acc SMALLINT, pub_rec SMALLINT, revol_bal FLOAT (2), revol_util FLOAT (2), total_acc SMALLINT, initial_list_status VARCHAR (1), out_prncp FLOAT (2), out_prncp_inv FLOAT (2), total_pymnt FLOAT (2), total_pymnt_inv FLOAT (2), total_rec_prncp FLOAT (2), total_rec_int FLOAT (2), total_rec_late_fee FLOAT (2), recoveries VARCHAR, collection_recovery_fee NUMERIC, last_pymnt_d VARCHAR (10), last_pymnt_amnt FLOAT (2), next_pymnt_d VARCHAR (10), last_credit_pull_d VARCHAR (10), collections_12_mths_ex_med SMALLINT, mths_since_last_major_derog SMALLINT, policy_code VARCHAR (10), application_type VARCHAR (20), annual_inc_joint FLOAT (2), dti_joint FLOAT (2), verification_status_joint VARCHAR (20), acc_now_delinq SMALLINT, tot_coll_amt VARCHAR, tot_cur_bal VARCHAR, open_acc_6m SMALLINT, open_act_il SMALLINT, open_il_12m SMALLINT, open_il_24m SMALLINT, mths_since_rcnt_il SMALLINT, total_bal_il VARCHAR, il_util VARCHAR, open_rv_12m SMALLINT, open_rv_24m SMALLINT, max_bal_bc VARCHAR, all_util SMALLINT, total_rev_hi_lim VARCHAR, inq_fi SMALLINT, total_cu_tl SMALLINT, inq_last_12m SMALLINT, acc_open_past_24mths SMALLINT, avg_cur_bal VARCHAR, bc_open_to_buy VARCHAR, bc_util FLOAT (2), chargeoff_within_12_mths SMALLINT, delinq_amnt VARCHAR, mo_sin_old_il_acct SMALLINT, mo_sin_old_rev_tl_op SMALLINT, mo_sin_rcnt_rev_tl_op SMALLINT, mo_sin_rcnt_tl SMALLINT, mort_acc SMALLINT, mths_since_recent_bc SMALLINT, mths_since_recent_bc_dlq SMALLINT, mths_since_recent_inq SMALLINT, mths_since_recent_revol_delinq SMALLINT, num_accts_ever_120_pd SMALLINT, num_actv_bc_tl SMALLINT, num_actv_rev_tl SMALLINT, num_bc_sats SMALLINT, num_bc_tl SMALLINT, num_il_tl SMALLINT, num_op_rev_tl SMALLINT, num_rev_accts SMALLINT, num_rev_tl_bal_gt_0 SMALLINT, num_sats SMALLINT, num_tl_120dpd_2m SMALLINT, num_tl_30dpd SMALLINT, num_tl_90g_dpd_24m SMALLINT, num_tl_op_past_12m SMALLINT, pct_tl_nvr_dlq FLOAT (2), percent_bc_gt_75 FLOAT (2), pub_rec_bankruptcies SMALLINT, tax_liens SMALLINT, tot_hi_cred_lim VARCHAR, total_bal_ex_mort VARCHAR, total_bc_limit VARCHAR, total_il_high_credit_limit VARCHAR, revol_bal_joint VARCHAR, sec_app_earliest_cr_line VARCHAR (10), sec_app_inq_last_6mths SMALLINT, sec_app_mort_acc SMALLINT, sec_app_open_acc SMALLINT, sec_app_revol_util FLOAT (2), sec_app_open_act_il SMALLINT, sec_app_num_rev_accts SMALLINT, sec_app_chargeoff_within_12_mths SMALLINT, sec_app_collections_12_mths_ex_med SMALLINT, sec_app_mths_since_last_major_derog SMALLINT, hardship_flag VARCHAR (1), hardship_type VARCHAR (50), hardship_reason VARCHAR (30), hardship_status VARCHAR (10), deferral_term SMALLINT, hardship_amount FLOAT (2), hardship_start_date VARCHAR, hardship_end_date VARCHAR, payment_plan_start_date VARCHAR, hardship_length SMALLINT, hardship_dpd SMALLINT, hardship_loan_status VARCHAR (30), orig_projected_additional_accrued_interest FLOAT (2), hardship_payoff_balance_amount FLOAT (2), hardship_last_payment_amount FLOAT (2), disbursement_method VARCHAR (10), debt_settlement_flag VARCHAR (1), debt_settlement_flag_date VARCHAR, settlement_status VARCHAR (10), settlement_date VARCHAR, settlement_amount FLOAT (2), settlement_percentage FLOAT (2), settlement_term SMALLINT);"
    pg_engine.execute(create_query)

    return True


def populate_loans_table(pg_engine):
    """
    Populates loans table in PostgreSQL
    :param pg_engine: PostgreSQL engine object
    :return: True
    """
    # set loan csv file path
    file = "../data/loan.csv"

    # define copy statement
    build_query = "COPY loans FROM STDIN DELIMITER ',' CSV HEADER"

    # leverage psycopg2 connection as context manager
    raw_con = pg_engine.raw_connection()
    with open(file) as f, \
            raw_con.connection, \
            raw_con.cursor() as cursor:
        cursor.copy_expert(build_query, f)

    return True


if __name__ == "__main__":
    """
    Driver script
    """
    loan_engine = create_db_engine()
    create_loans_table(loan_engine)
    populate_loans_table(loan_engine)
