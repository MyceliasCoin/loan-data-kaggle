import pandas as pd
from sqlalchemy import create_engine
import boto3


def create_db_engine():
    """
    Connects to PostgreSQL instance
    :return: PostgreSQL engine object
    """
    engine = create_engine('postgres://postgres:postgres@ec2-3-83-202-20.compute-1.amazonaws.com:5432/loan_db')
    return engine


def get_csv_from_s3():
    """
    Leverages boto3 to connect to S3 bucket using AWS credentials
    :return: S3 bucket object
    """
    client = boto3.client('s3')
    obj = client.get_object(Bucket='loan-data-test', Key='loan.csv')
    return obj


def create_df(s3_object):
    """
    Loads S3 bucket object into pandas DataFrame
    :param s3_object: s3 object
    :return: pandas DataFrame
    """
    # prespecify datatypes for more efficient loading given limited memory
    dtypes = {'id': 'float64', 'member_id': 'float64', 'loan_amnt': 'int64', 'funded_amnt': 'int64', 'funded_amnt_inv': 'float64', 'term': 'object', 'int_rate': 'float64', 'installment': 'float64', 'grade': 'object', 'sub_grade': 'object', 'emp_title': 'object', 'emp_length': 'object', 'home_ownership': 'object', 'annual_inc': 'float64', 'verification_status': 'object', 'issue_d': 'object', 'loan_status': 'object', 'pymnt_plan': 'object', 'url': 'float64', 'desc': 'object', 'purpose': 'object', 'title': 'object', 'zip_code': 'object', 'addr_state': 'object', 'dti': 'float64', 'delinq_2yrs': 'float64', 'earliest_cr_line': 'object', 'inq_last_6mths': 'float64', 'mths_since_last_delinq': 'float64', 'mths_since_last_record': 'float64', 'open_acc': 'float64', 'pub_rec': 'float64', 'revol_bal': 'int64', 'revol_util': 'float64', 'total_acc': 'float64', 'initial_list_status': 'object', 'out_prncp': 'float64', 'out_prncp_inv': 'float64', 'total_pymnt': 'float64', 'total_pymnt_inv': 'float64', 'total_rec_prncp': 'float64', 'total_rec_int': 'float64', 'total_rec_late_fee': 'float64', 'recoveries': 'float64', 'collection_recovery_fee': 'float64', 'last_pymnt_d': 'object', 'last_pymnt_amnt': 'float64', 'next_pymnt_d': 'object', 'last_credit_pull_d': 'object', 'collections_12_mths_ex_med': 'float64', 'mths_since_last_major_derog': 'float64', 'policy_code': 'int64', 'application_type': 'object', 'annual_inc_joint': 'float64', 'dti_joint': 'float64', 'verification_status_joint': 'object', 'acc_now_delinq': 'float64', 'tot_coll_amt': 'float64', 'tot_cur_bal': 'float64', 'open_acc_6m': 'float64', 'open_act_il': 'float64', 'open_il_12m': 'float64', 'open_il_24m': 'float64', 'mths_since_rcnt_il': 'float64', 'total_bal_il': 'float64', 'il_util': 'float64', 'open_rv_12m': 'float64', 'open_rv_24m': 'float64', 'max_bal_bc': 'float64', 'all_util': 'float64', 'total_rev_hi_lim': 'float64', 'inq_fi': 'float64', 'total_cu_tl': 'float64', 'inq_last_12m': 'float64', 'acc_open_past_24mths': 'float64', 'avg_cur_bal': 'float64', 'bc_open_to_buy': 'float64', 'bc_util': 'float64', 'chargeoff_within_12_mths': 'float64', 'delinq_amnt': 'float64', 'mo_sin_old_il_acct': 'float64', 'mo_sin_old_rev_tl_op': 'float64', 'mo_sin_rcnt_rev_tl_op': 'float64', 'mo_sin_rcnt_tl': 'float64', 'mort_acc': 'float64', 'mths_since_recent_bc': 'float64', 'mths_since_recent_bc_dlq': 'float64', 'mths_since_recent_inq': 'float64', 'mths_since_recent_revol_delinq': 'float64', 'num_accts_ever_120_pd': 'float64', 'num_actv_bc_tl': 'float64', 'num_actv_rev_tl': 'float64', 'num_bc_sats': 'float64', 'num_bc_tl': 'float64', 'num_il_tl': 'float64', 'num_op_rev_tl': 'float64', 'num_rev_accts': 'float64', 'num_rev_tl_bal_gt_0': 'float64', 'num_sats': 'float64', 'num_tl_120dpd_2m': 'float64', 'num_tl_30dpd': 'float64', 'num_tl_90g_dpd_24m': 'float64', 'num_tl_op_past_12m': 'float64', 'pct_tl_nvr_dlq': 'float64', 'percent_bc_gt_75': 'float64', 'pub_rec_bankruptcies': 'float64', 'tax_liens': 'float64', 'tot_hi_cred_lim': 'float64', 'total_bal_ex_mort': 'float64', 'total_bc_limit': 'float64', 'total_il_high_credit_limit': 'float64', 'revol_bal_joint': 'float64', 'sec_app_earliest_cr_line': 'object', 'sec_app_inq_last_6mths': 'float64', 'sec_app_mort_acc': 'float64', 'sec_app_open_acc': 'float64', 'sec_app_revol_util': 'float64', 'sec_app_open_act_il': 'float64', 'sec_app_num_rev_accts': 'float64', 'sec_app_chargeoff_within_12_mths': 'float64', 'sec_app_collections_12_mths_ex_med': 'float64', 'sec_app_mths_since_last_major_derog': 'float64', 'hardship_flag': 'object', 'hardship_type': 'object', 'hardship_reason': 'object', 'hardship_status': 'object', 'deferral_term': 'float64', 'hardship_amount': 'float64', 'hardship_start_date': 'object', 'hardship_end_date': 'object', 'payment_plan_start_date': 'object', 'hardship_length': 'float64', 'hardship_dpd': 'float64', 'hardship_loan_status': 'object', 'orig_projected_additional_accrued_interest': 'float64', 'hardship_payoff_balance_amount': 'float64', 'hardship_last_payment_amount': 'float64', 'disbursement_method': 'object', 'debt_settlement_flag': 'object', 'debt_settlement_flag_date': 'object', 'settlement_status': 'object', 'settlement_date': 'object', 'settlement_amount': 'float64', 'settlement_percentage': 'float64', 'settlement_term': 'float64'}

    # read in loan csv in chunks and append to DataFrame
    loan_chunk = pd.read_csv(s3_object['Body'], dtype=dtypes, chunksize=100000)
    chunk_list = []
    for chunk in loan_chunk:
        chunk_list.append(chunk)
    loan_df = pd.concat(chunk_list)

    print(loan_df.head())

    return loan_df


def write_df_to_postgres(dataframe, pg_engine):
    """
    Writes DataFrame to appropriate PostgreSQL table
    :param dataframe: pandas DataFrame
    :param pg_engine: postgres engine object
    :return:
    """
    # dataframe.to_sql('loans_test', con=pg_engine, if_exists='replace', chunksize=100000)
    dataframe.to_sql('loans_test', pg_engine, index=False, chunksize=100000)


if __name__ == "__main__":
    """
    Driver script for main function
    """
    engine = create_db_engine()
    s3_obj = get_csv_from_s3()
    loan_data = create_df(s3_obj)
    write_df_to_postgres(loan_data, engine)
