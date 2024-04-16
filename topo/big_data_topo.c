//Code ：Low-voltage multi-level topology identification algorithm
//Time ：2023 - 11 
//Author ： Intelligent algorithms group
//Version ： 1.1
//Copyright ：TOPSCOMM Co.ltd
//代码 ：低压多级拓扑识别算法
//时间 ：2023 - 11
//作者 ：智能算法部
//版本 ：1.1
//版权 ：青岛鼎信通讯股份有限公司
//#define __USE_IN_EMBEDDED_    // 开启支持嵌入式编译
#define DEBUG                 // 开启表示调试程序用


#include <stdlib.h>
#include "string.h"
#include <stdio.h>
#include "topo.h"

#ifndef Used_in_Terminal
static sqlite3* db = NULL;
#endif

int Init_Sql_Tabel()
{
	int(*callback)(void*, int, char**, char**) = NULL;   /* Callback function */
	char sql[500] = { 0 };
	char* errmsg = NULL;
	int ret = 0;

	const char* sql_delete_data_act_ori = "DELETE FROM BIG_DATA_TOPO_ACT; ";
	const char* sql_create_data_act_ori = "CREATE TABLE IF NOT EXISTS BIG_DATA_TOPO_ACT(IDX INTEGER  PRIMARY KEY,Child_no INTEGER, ACTIVE_POWER INTEGER,TIMES INTEGER); ";

	const char* sql_delete_data_vol_ori = "DELETE FROM BIG_DATA_TOPO_VOL; ";
	const char* sql_create_data_vol_ori = "CREATE TABLE IF NOT EXISTS BIG_DATA_TOPO_VOL(IDX INTEGER  PRIMARY KEY,Child_no INTEGER,  VOLTAGE_A INTEGER,VOLTAGE_B INTEGER,VOLTAGE_C INTEGER,TIMES INTEGER); ";

	const char* sql_delete_archives = "DELETE FROM ARCHIVES; ";
	const char* sql_create_archives = "CREATE TABLE IF NOT EXISTS ARCHIVES(IDX INTEGER  PRIMARY KEY,Child_No INTEGER,CT INTEGER,PT INTEGER,DEVID_TYPE INTEGER); ";

	const char* sql_delete_rsult = "DELETE FROM BIG_DATA_RESULTS; ";
	const char* sql_create_rsult = "CREATE TABLE IF NOT EXISTS BIG_DATA_RESULTS(IDX INTEGER  PRIMARY KEY,Child_No INTEGER,Parents_No INTEGER); ";
#ifndef Used_in_Terminal

	//删除旧表
	ret = sqlite3_exec(db, sql_delete_data_vol_ori, callback, 0, &errmsg);  // SQLite 数据库库中的一个函数，用于执行 SQL 命令
	if (ret != SQLITE_OK)
	{
		sqlite3_free(errmsg);
	}

	//创建新表-check
	ret = sqlite3_exec(db, sql_create_data_vol_ori, callback, 0, &errmsg);
	if (ret != SQLITE_OK)
	{
		sqlite3_free(errmsg);
	}
	//删除旧表
	ret = sqlite3_exec(db, sql_delete_data_act_ori, callback, 0, &errmsg);
	if (ret != SQLITE_OK)
	{
		sqlite3_free(errmsg);
	}

	//创建新表-check
	ret = sqlite3_exec(db, sql_create_data_act_ori, callback, 0, &errmsg);
	if (ret != SQLITE_OK)
	{
		sqlite3_free(errmsg);
	}

	//删除旧表
	ret = sqlite3_exec(db, sql_delete_archives, callback, 0, &errmsg);
	if (ret != SQLITE_OK)
	{
		//fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(errmsg);
		//_WARN_PRINT(" sql_delete_table harmonic_analysis err\n");
		//        return FALSE;
	}
	else
	{
		//_WARN_PRINT(" sql_delete_table harmonic_analysis sucess\n");
		//fprintf(stdout, "Table delete successfully\n");
	}

	//创建新表-check
	ret = sqlite3_exec(db, sql_create_archives, callback, 0, &errmsg);
	if (ret != SQLITE_OK)
	{
		sqlite3_free(errmsg);
		//_WARN_PRINT(" sql_create_table harmonic_analysis err\n");
		//return FALSE;
	}
	else
	{
		//_WARN_PRINT(" sql_create_table harmonic_analysis sucess\n");
	}

	//删除旧表
	ret = sqlite3_exec(db, sql_delete_rsult, callback, 0, &errmsg);
	if (ret != SQLITE_OK)
	{
		//fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(errmsg);
		//_WARN_PRINT(" sql_delete_table TABLE_DATA_LOOK_TOPS err\n");
		//        return FALSE;
	}
	else
	{
		//_WARN_PRINT(" sql_delete_table TABLE_DATA_LOOK_TOPS sucess\n");
		//fprintf(stdout, "Table delete successfully\n");
	}
	//创建新表-check
	ret = sqlite3_exec(db, sql_create_rsult, callback, 0, &errmsg);
	if (ret != SQLITE_OK)
	{
		sqlite3_free(errmsg);
		//_WARN_PRINT(" sql_create_table_check TABLE_DATA_LOOK_TOPS err\n");
		//return FALSE;
	}
	else
	{
		//_WARN_PRINT(" sql_create_table_check TABLE_DATA_LOOK_TOPS sucess\n");
	}
#else
	//删除旧表
	ret = db_sql_exec(topolib_db, sql_delete_data_vol_ori, &errmsg);
	if (ret != SQLITE_OK)
	{
		sqlite3_free(errmsg);
	}

	//创建新表-check
	ret = db_sql_exec(topolib_db, sql_create_data_vol_ori, &errmsg);
	if (ret != SQLITE_OK)
	{
		sqlite3_free(errmsg);
	}
	//删除旧表
	ret = db_sql_exec(topolib_db, sql_delete_data_act_ori, &errmsg);
	if (ret != SQLITE_OK)
	{
		sqlite3_free(errmsg);
	}

	//创建新表-check
	ret = db_sql_exec(topolib_db, sql_create_data_act_ori, &errmsg);
	if (ret != SQLITE_OK)
	{
		sqlite3_free(errmsg);
	}

	//删除旧表
	ret = db_sql_exec(topolib_db, sql_delete_archives, &errmsg);
	if (ret != SQLITE_OK)
	{
		//fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(errmsg);
		//_WARN_PRINT(" sql_delete_table harmonic_analysis err\n");
		//        return FALSE;
	}
	else
	{
		//_WARN_PRINT(" sql_delete_table harmonic_analysis sucess\n");
		//fprintf(stdout, "Table delete successfully\n");
	}

	//创建新表-check
	ret = db_sql_exec(topolib_db, sql_create_archives, &errmsg);
	if (ret != SQLITE_OK)
	{
		sqlite3_free(errmsg);
		//_WARN_PRINT(" sql_create_table harmonic_analysis err\n");
		//return FALSE;
	}
	else
	{
		//_WARN_PRINT(" sql_create_table harmonic_analysis sucess\n");
	}

	//删除旧表
	ret = db_sql_exec(topolib_db, sql_delete_rsult, &errmsg);
	if (ret != SQLITE_OK)
	{
		//fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(errmsg);
		//_WARN_PRINT(" sql_delete_table TABLE_DATA_LOOK_TOPS err\n");
		//        return FALSE;
	}
	else
	{
		//_WARN_PRINT(" sql_delete_table TABLE_DATA_LOOK_TOPS sucess\n");
		//fprintf(stdout, "Table delete successfully\n");
	}
	//创建新表-check
	ret = db_sql_exec(topolib_db, sql_create_rsult, &errmsg);
	if (ret != SQLITE_OK)
	{
		sqlite3_free(errmsg);
		//_WARN_PRINT(" sql_create_table_check TABLE_DATA_LOOK_TOPS err\n");
		//return FALSE;
	}
	else
	{
		//_WARN_PRINT(" sql_create_table_check TABLE_DATA_LOOK_TOPS sucess\n");
	}
#endif
	return 0;
}

void Write_To_Final_Result_Table(topo_result_t* result, INT16U couple_num)
{
	char sql[500] = { 0 };
	char* errmsg = NULL;
	int i = 0;
	int ret = 0;
	const char* sql_insert_final_result_table = "INSERT INTO BIG_DATA_RESULTS(idx, child_no, parents_no, child_no_layer_num, sure_flag, max_parent_id) values (%d,%d,%d,%d,%d);";
#ifndef Used_in_Terminal
	ret = sqlite3_exec(db, "BEGIN;", 0, 0, &errmsg);
	if (ret != SQLITE_OK)
	{
		sqlite3_free(errmsg);
	}
	for (i = 0; i < couple_num; i++)
	{
		memset(sql, 0, sizeof(sql));
		// snprintf(sql, sizeof(sql), sql_insert_final_result_table, i, result->result_meas_no[i][0], result->result_meas_no[i][1]);
		snprintf(sql, sizeof(sql), sql_insert_final_result_table, i, result->topo_result[i][0], result->topo_result[i][1], result->topo_result[i][2], result->topo_result[i][3], result->topo_result[i][4]);
		ret = sqlite3_exec(db, sql, 0, 0, &errmsg);
		if (ret != SQLITE_OK)
		{
			sqlite3_free(errmsg);
		}
	}
	ret = sqlite3_exec(db, "COMMIT;", 0, 0, &errmsg);
	if (ret != SQLITE_OK)
	{
		sqlite3_free(errmsg);
	}
#else
	ret = db_sql_exec(topolib_db, "BEGIN;", &errmsg);
	if (ret != SQLITE_OK)
	{
		sqlite3_free(errmsg);
	}
	for (i = 0; i < couple_num; i++)
	{
		memset(sql, 0, sizeof(sql));
		snprintf(sql, sizeof(sql), sql_insert_final_result_table, i, result->result_meas_no[i][0], result->result_meas_no[i][1]);
		ret = db_sql_exec(topolib_db, sql, &errmsg);
		if (ret != SQLITE_OK)
		{
			sqlite3_free(errmsg);
		}
	}
	ret = db_sql_exec(topolib_db, "COMMIT;", &errmsg);
	if (ret != SQLITE_OK)
	{
		sqlite3_free(errmsg);
	}

#endif
}

void Write_To_Data_Table(topo_data_t* data, topo_voltage_t* voltage)
{
	char sql[500] = { 0 };
	char* errmsg = NULL;
	int ret = 0;
	int i = 0, j = 0;
	INT16U sig_num = 0;
	INT16U tri_num = 0;
	int used_sample_num = 0;
	const char* sql_insert_data_table = "insert into  BIG_DATA_TOPO_ACT(Child_no,ACTIVE_POWER,times) values(%d,%f,%d)";
	const char* sql_insert_data_vol_table = "insert into  BIG_DATA_TOPO_VOL(Child_no,VOLTAGE_A,VOLTAGE_B,VOLTAGE_C,times) values(%d,%f,%f,%f,%d)";
	const char* sql_insert_ar_table = "insert into  ARCHIVES(Child_no,CT,PT,DEVID_TYPE) values(%d,%d,%d,%d)";
#ifndef  Used_in_Terminal

	ret = sqlite3_exec(db, "BEGIN;", 0, 0, &errmsg);
	if (ret != SQLITE_OK)
	{
		sqlite3_free(errmsg);
	}
	//写入功率数据
	for (i = 0; i < data->used_sample_num; i++)
	{
		for (j = 0; j < data->used_meter_num; j++)
		{
			memset(sql, 0, sizeof(sql));
			snprintf(sql, sizeof(sql), sql_insert_data_table, data->measure_point_no[j], data->child_meter_ori[i][j], i + 1);
			ret = sqlite3_exec(db, sql, 0, 0, &errmsg);
			if (ret != SQLITE_OK)
			{
				sqlite3_free(errmsg);
			}
		}
	}
	//统计单相表和三相表个数
	for (i = 0; i < data->used_meter_num; i++)
	{
		if (0 == data->device_type[i])
			sig_num = sig_num + 1;
		if (0 != data->device_type[i])
			tri_num = tri_num + 1;
	}
	//写入电压数据
	for (i = 0; i < data->used_voltage_num; i++)
	{

		for (j = 0; j < sig_num; j++)
		{
			memset(sql, 0, sizeof(sql));
			snprintf(sql, sizeof(sql), sql_insert_data_vol_table, data->measure_point_no[voltage->ind_SINGLE[j]], voltage->voltage_SINGLE[i][j], 0.0f, 0.0f, i + 1);
			ret = sqlite3_exec(db, sql, 0, 0, &errmsg);
			if (ret != SQLITE_OK)
			{
				sqlite3_free(errmsg);
			}
		}
		for (j = 0; j < tri_num; j++)
		{
			memset(sql, 0, sizeof(sql));
			snprintf(sql, sizeof(sql), sql_insert_data_vol_table, data->measure_point_no[voltage->ind_TRIPLE[j]], voltage->voltage_TRIPLE[i][j][0], voltage->voltage_TRIPLE[i][j][1], voltage->voltage_TRIPLE[i][j][2], i + 1);
			ret = sqlite3_exec(db, sql, 0, 0, &errmsg);
			if (ret != SQLITE_OK)
			{
				sqlite3_free(errmsg);
			}
		}
	}
	//写入档案数据
	for (i = 0; i < data->used_meter_num; i++)
	{
		memset(sql, 0, sizeof(sql));
		snprintf(sql, sizeof(sql), sql_insert_ar_table, data->measure_point_no[i], data->CT[i], data->PT[i], data->device_type[i]);
		ret = sqlite3_exec(db, sql, 0, 0, &errmsg);
		if (ret != SQLITE_OK)
		{
			sqlite3_free(errmsg);
		}

	}
	ret = sqlite3_exec(db, "COMMIT;", 0, 0, &errmsg);
	if (ret != SQLITE_OK)
	{
		sqlite3_free(errmsg);
	}
#else

	ret = db_sql_exec(topolib_db, "BEGIN;", &errmsg);
	if (ret != SQLITE_OK)
	{
		sqlite3_free(errmsg);
	}
	//写入功率数据
	for (i = 0; i < data->used_sample_num; i++)
	{
		for (j = 0; j < data->used_meter_num; j++)
		{
			memset(sql, 0, sizeof(sql));
			snprintf(sql, sizeof(sql), sql_insert_data_table, data->measure_point_no[j], data->child_meter_ori[i][j], i + 1);
			ret = db_sql_exec(topolib_db, sql, &errmsg);
			if (ret != SQLITE_OK)
			{
				sqlite3_free(errmsg);
			}
		}
	}
	//统计单相表和三相表个数
	for (i = 0; i < data->used_meter_num; i++)
	{
		if (0 == data->device_type[i])
			sig_num = sig_num + 1;
		if (0 != data->device_type[i])
			tri_num = tri_num + 1;
	}
	//写入电压数据
	for (i = 0; i < data->used_voltage_num; i++)
	{

		for (j = 0; j < sig_num; j++)
		{
			memset(sql, 0, sizeof(sql));
			snprintf(sql, sizeof(sql), sql_insert_data_vol_table, data->measure_point_no[voltage->ind_SINGLE[j]], voltage->voltage_SINGLE[i][j], 0.0f, 0.0f, i + 1);
			ret = db_sql_exec(topolib_db, sql, &errmsg);
			if (ret != SQLITE_OK)
			{
				sqlite3_free(errmsg);
			}
		}
		for (j = 0; j < tri_num; j++)
		{
			memset(sql, 0, sizeof(sql));
			snprintf(sql, sizeof(sql), sql_insert_data_vol_table, data->measure_point_no[voltage->ind_TRIPLE[j]], voltage->voltage_TRIPLE[i][j][0], voltage->voltage_TRIPLE[i][j][1], voltage->voltage_TRIPLE[i][j][2], i + 1);
			ret = db_sql_exec(topolib_db, sql, &errmsg);
			if (ret != SQLITE_OK)
			{
				sqlite3_free(errmsg);
			}
		}
	}
	//写入档案数据
	for (i = 0; i < data->used_meter_num; i++)
	{
		memset(sql, 0, sizeof(sql));
		snprintf(sql, sizeof(sql), sql_insert_ar_table, data->measure_point_no[i], data->CT[i], data->PT[i], data->device_type[i]);
		ret = db_sql_exec(topolib_db, sql, &errmsg);
		if (ret != SQLITE_OK)
		{
			sqlite3_free(errmsg);
		}

	}
	ret = db_sql_exec(topolib_db, "COMMIT;", &errmsg);
	if (ret != SQLITE_OK)
	{
		sqlite3_free(errmsg);
	}
#endif
}

/* ***********************************************************
正常数据向前移动子函数
函数功能：将索引srcIdx数据复制到dstIdx索引处
输入：索引位置srcIdx、dstIdx，结构体变量data
输出：dstIdx + 1,更新结构体变量data、cal_para
*********************************************************** */
static INT16U data_move(INT16U srcIdx, INT16U dstIdx, topo_data_t* data)
{
	INT16U j;
	if (dstIdx != srcIdx)
	{
		for (j = 0; j < data->used_meter_num; j++)
		{
			data->child_meter[dstIdx][j] = data->child_meter[srcIdx][j];
		}
	}
	return dstIdx + 1;
}


/* ***********************************************************
清理数据子函数
函数功能：清理数据，得到剔除异常点后的数据
输入：索引位置startIdx，结构体变量data
输出：更新结构体变量data、cal_para
*********************************************************** */
static void data_clear(INT16U startIdx, topo_data_t* data)
{
	INT16U i, j;

	for (i = startIdx; i < data->used_sample_num; i++)
	{
		for (j = 0; j < data->used_meter_num; j++)
		{
			data->child_meter[i][j] = 0;
		}
	}
}

/* ***********************************************************************
   求协方差的子函数
   函数功能：求两个数组的协方差
   输入：两个数组x[]、y[]，数组长度n
   输出：两个数组的协方差cox_value
************************************************************************ */
static void cox(FP64 x[], FP64 y[], INT16U n, FP64* cox_value)
{
	FP64 averx = 0.0f;
	FP64 avery = 0.0f;
	FP64 sumx = 0.0f;
	FP64 sumy = 0.0f;
	FP64 sum = 0.0f;
	INT16U p;

	for (p = 0; p < n; p++)
	{
		sumx += x[p];
		sumy += y[p];
	}
	averx = sumx / n;
	avery = sumy / n;
	for (p = 0; p < n; p++)
	{
		sum += (x[p] - averx) * (y[p] - avery);
	}
	*cox_value = sum / (n - 1);
}

/* ***********************************************************************
   求皮尔逊相似度的子函数
   函数功能：求两个数组的皮尔逊相似度
   输入：两个数组x[] y[]、数组长度n
   输出：两个数组的皮尔逊相似度
************************************************************************ */
static FP64 rhofunction(FP64 x[], FP64 y[], INT16U n)
{
	FP64 standard_x = 0.0f;
	FP64 standard_y = 0.0f;
	FP64 cox_xy = 0.0f;

	cox(x, x, n, &standard_x);
	cox(y, y, n, &standard_y);
	cox(x, y, n, &cox_xy);

	if (standard_x * standard_y > 0.000001)
	{
		return cox_xy / (FP64)(sqrt(standard_x * standard_y));
	}
	else
	{
		return 0;
	}
}

/* ***********************************************************************
   将一组数做 0-1 归一化的子函数
   函数功能：将一组数做 0-1 归一化
   输入：数组x[]，数组长度n
   输出：归一化后的数组x[]
************************************************************************ */
static void normalization_function(FP64 x[], INT16U n)
{
	FP64 max_1 = 0.0f;
	FP64 min_1 = 99999.0f;
	INT16U i;

	for (i = 0; i < n; i++)
	{
		if (x[i] > max_1)
		{
			max_1 = x[i];
		}

		if (x[i] < min_1)
		{
			min_1 = x[i];
		}
	}

	for (i = 0; i < n; i++)
	{
		x[i] = (x[i] - min_1) / (max_1 - min_1);
	}
}

/* ***********************************************************
   降序子函数
   函数功能：序列按降序排列
   输入：一组数据dataArr[]、数组长度length
   输出：X分位数
*********************************************************** */
static FP64 descend_function(FP64 dataArr[], INT16U length)
{
	FP64 Temp_value = 0.0;
	INT16U i, j;

	for (i = 0; i < length - 1; i++)
	{
		for (j = i + 1; j < length; j++)
		{
			Temp_value = 0;
			if (dataArr[i] < dataArr[j])
			{
				Temp_value = dataArr[i];
				dataArr[i] = dataArr[j];
				dataArr[j] = Temp_value;
			}
		}
	}
	return 0;
}

/* ***********************************************************
   求分位数子函数
   函数功能：求一组数据的分位数
   输入：一组数据dataArr[]、数组长度length、求X分位数(X=(1-pram2))
   输出：X分位数
*********************************************************** */
static FP64 quantile_function(FP64 dataArr[], INT16U length, FP64 pram2)
{
	FP64 Temp_value = 0.0f;
	FP64 r = 0.0f;
	FP64 qua_value = 0.0f;
	INT16U Temp = 0;
	INT16U i, j, k, kp1;

	for (i = 0; i < length - 1; i++)
	{
		for (j = i + 1; j < length; j++)
		{
			Temp_value = 0;
			if (dataArr[i] > dataArr[j])
			{
				Temp_value = dataArr[i];
				dataArr[i] = dataArr[j];
				dataArr[j] = Temp_value;
			}
		}
	}
	r = pram2 * length;
	k = (INT16U)floor(r + 0.5f);
	kp1 = k + 1;
	r = r - k;
	kp1 = (kp1 < length) ? kp1 : length;


	Temp = kp1;
	if (Temp > length)
	{
		Temp = length;
	}
	if (Temp < 1)
	{
		Temp = 1;
	}

	qua_value = (0.5f + r) * dataArr[Temp - 1] + (0.5f - r) * dataArr[k - 1];

	return qua_value;
}

/* ***********************************************************
   数据预处理函数
   函数功能：剔除部分异常点
   输入：结构体变量 data cal_para
   输出：更新后的 data
*********************************************************** */
static void data_preprocessing(topo_data_t* data, topo_cal_para_t* cal_para)
{
#define METER_SELF_CONSUMPTION     (0.0288f)		// 单相表自身用电
#define ERROR_RATE_THRESHOLD       (0.10f)       	// 线损率阈值
#define PARENT_METER_THRESHOLD     (1.0f)		// 总表电量阈值

	FP64 sum_child = 0.0f;
	FP64* parent_meter = &(cal_para->parent_meter[0]);
	FP64* error = &(cal_para->fit[0]);
	FP64* error_rate = &(cal_para->weight[0]);
	INT16U i, j;
	INT16U used_sample_num = data->used_sample_num;
	INT16U delete_num = 0;					// 待剔除的数据点数
	INT16U K = 0;						// 计数器，如果满足条件保留并自加1

	/* 乘CT PT */
	for (i = 0; i < used_sample_num; i++)
	{
		for (j = 0; j < data->used_meter_num; j++)
		{
			data->child_meter_ori[i][j] *= data->CT[j] * data->PT[j];
		}
	}

	/* 求和，赋值给临时变量，计算线损和线损率 */
	for (i = 0; i < used_sample_num; i++)
	{
		sum_child = 0;
		for (j = 0; j < data->used_meter_num; j++)
		{
			if (data->device_type[j] == TOPO_METER_SINGLE || data->device_type[j] == TOPO_METER_THREE)
			{
				sum_child += data->child_meter_ori[i][j];
			}

			if (data->device_type[j] == TOPO_MASTER_METER)
			{
				parent_meter[i] = data->child_meter_ori[i][j];
			}
		}
		error[i] = parent_meter[i] - sum_child;
		error_rate[i] = error[i] / (parent_meter[i] + 0.00001);
	}

	for (i = 0; i < used_sample_num; i++)
	{
		/* 异常数据检测 */
		if (error_rate[i] < 0 || error_rate[i] > ERROR_RATE_THRESHOLD || parent_meter[i] < PARENT_METER_THRESHOLD)
		{
			delete_num += 1;
		}
		else
		{
			/* 正常数据前移，覆盖异常数据 */
			K = data_move(i, K, data);		 // 若i！= K，i 位置数据向前移动到 K 位置数据上
		}
	}

	/* 整理数据 */
	if (delete_num > 0)
	{
		data_clear(K, data);				// 将 K 位置开始的数据全部置零
	}

	/* 更新可用采样点数 */
	data->used_sample_num = used_sample_num - delete_num;

	memset(parent_meter, 0, sizeof(*parent_meter) * TOPO_SAMPLE_NUM);
	memset(error, 0, sizeof(*error) * TOPO_SAMPLE_NUM);
	memset(error_rate, 0, sizeof(*error_rate) * TOPO_SAMPLE_NUM);

#undef METER_SELF_CONSUMPTION
#undef ERROR_RATE_THRESHOLD
#undef PARENT_METER_THRESHOLD

}

/* ***********************************************************************
矩阵求逆子函数(极限省内存版)
函数功能：求一个矩阵的逆矩阵
输入：矩阵a，矩阵维度N
输出：矩阵a的逆矩阵a_inverse
************************************************************************ */
static INT16U matrix_inverse_storage(FP64* a, topo_huberfun_para_t* para)
{
	INT16S i, j, k;
	INT16U used_num = para->used_num;
	INT16U* used_id = para->used_id;
	FP64 s;
	FP64* U = para->U;

	memset(U, 0, sizeof(*U) * TOPO_METER_NUM * TOPO_METER_NUM);

	for (i = 0; i < used_num; i++)
	{
		for (j = i; j < used_num; j++)
		{
			s = 0.0f;
			for (k = 0; k < i; k++)
			{
				s += U[used_id[i] * TOPO_METER_NUM + used_id[k]] * U[used_id[k] * TOPO_METER_NUM + used_id[j]];
			}

			U[used_id[i] * TOPO_METER_NUM + used_id[j]] = a[used_id[i] * TOPO_METER_NUM + used_id[j]] - s;		// 按行计算U值
		}

		for (j = i + 1; j < used_num; j++)
		{
			s = 0.0f;
			for (k = 0; k < i; k++)
			{
				s += U[used_id[j] * TOPO_METER_NUM + used_id[k]] * U[used_id[k] * TOPO_METER_NUM + used_id[i]];
			}
			U[used_id[j] * TOPO_METER_NUM + used_id[i]] = (a[used_id[j] * TOPO_METER_NUM + used_id[i]] - s) / U[used_id[i] * TOPO_METER_NUM + used_id[i]];		// 按列计算L值
		}
	}
	for (i = 0; i < used_num; i++)		 //按列序，列内按照从下到上，计算u的逆矩阵
	{
		U[used_id[i] * TOPO_METER_NUM + used_id[i]] = 1 / U[used_id[i] * TOPO_METER_NUM + used_id[i]];
	}

	memset(a, 0, sizeof(*a) * (TOPO_METER_NUM * TOPO_METER_NUM));		// 初始化为零，重新填充U的值

	for (i = 0; i < used_num; i++)
	{
		for (j = i; j < used_num; j++)
		{
			a[used_id[i] * TOPO_METER_NUM + used_id[j]] = U[used_id[i] * TOPO_METER_NUM + used_id[j]];
		}
	}

	for (i = 0; i < used_num; i++)		//计算l矩阵对角线
	{
		U[used_id[i] * TOPO_METER_NUM + used_id[i]] = 1.0f;
	}

	for (i = 1; i < used_num; i++)
	{
		for (j = 0; j < i; j++)
		{
			s = 0.0f;
			for (k = 0; k < i; k++)
			{
				if (k < j)
				{
					s = s + 0.0f;
				}
				else
				{
					s += U[used_id[i] * TOPO_METER_NUM + used_id[k]] * U[used_id[j] * TOPO_METER_NUM + used_id[k]];
				}
			}
			U[used_id[j] * TOPO_METER_NUM + used_id[i]] = -s;
		}
	}

	for (i = 1; i < used_num; i++)
	{
		for (j = 0; j < i; j++)
		{
			U[used_id[i] * TOPO_METER_NUM + used_id[j]] = 0.0;
		}
	}

	for (i = 1; i < used_num; i++)
	{
		for (j = i - 1; j >= 0; j--)
		{
			s = 0.0f;
			for (k = j + 1; k <= i; k++)
			{
				if (k > i)
				{
					s = s + 0.0f;
				}
				else
				{
					s += a[used_id[j] * TOPO_METER_NUM + used_id[k]] * a[used_id[i] * TOPO_METER_NUM + used_id[k]];
				}
			}
			a[used_id[i] * TOPO_METER_NUM + used_id[j]] = -s * a[used_id[j] * TOPO_METER_NUM + used_id[j]];
		}
	}

	for (i = 0; i < used_num; i++)
	{
		for (j = 0; j <= i; j++)
		{
			U[used_id[i] * TOPO_METER_NUM + used_id[j]] = a[used_id[i] * TOPO_METER_NUM + used_id[j]];
		}
	}

	memset(a, 0, sizeof(*a) * (TOPO_METER_NUM * TOPO_METER_NUM));		 // 初始化为零，重新填充逆矩阵的值

	for (i = 0; i < used_num; i++)		// 计算矩阵a的逆矩阵
	{
		for (j = 0; j < used_num; j++)
		{
			for (k = 0; k < used_num; k++)
			{
				if (k >= i && k >= j)
				{
					if (j == k)
					{
						a[used_id[i] * TOPO_METER_NUM + used_id[j]] += U[used_id[k] * TOPO_METER_NUM + used_id[i]];
					}
					else
					{
						a[used_id[i] * TOPO_METER_NUM + used_id[j]] += U[used_id[k] * TOPO_METER_NUM + used_id[i]] * U[used_id[j] * TOPO_METER_NUM + used_id[k]];
					}
				}
			}
		}
	}
	return 0;
}

static FP64 L1adaptive(topo_huberfun_para_t* para, FP64* beta_ridge)
{
	FP64* square_x = para->square_x;
	FP64 beta_down = 0.0f;
	FP64 square_x_down = 0.0f;
	FP64 square_max_2nd = 0.0f;
	FP64 L1_para = 0.0f;
	INT16U m = para->matrixpara->column1 - 1;
	INT16U n = para->matrixpara->row1;
	INT16U used_m = m;
	INT16U* used_id = para->used_id;
	INT16U i, j;

	for (i = 0; i < m; i++)
	{
		beta_ridge[used_id[i]] = (FP64)fabs(beta_ridge[used_id[i]]);
	}

	descend_function(square_x, m);		// 将x^2倒序排列

	square_max_2nd = square_x[1];
	for (i = 0; i < m; i++)
	{
		square_x[i] = 0.0f;
		for (j = 0; j < n; j++)
		{
			square_x[i] += para->X[j * TOPO_METER_NUM + used_id[i]] * para->X[j * TOPO_METER_NUM + used_id[i]];		//恢复顺序
		}
	}

	for (i = 0; i < m; i++)
	{
		if (square_x[i] < 0.001f * square_max_2nd)
		{
			used_m -= 1;
			beta_ridge[used_id[i]] = 0.0f;
		}
	}

	descend_function(square_x, m);								// 目的为倒序
	descend_function(beta_ridge, para->used_meter_num_ori);		// 倒序排列

	square_x_down = quantile_function(square_x, used_m, TOPO_L1_PARA_DOWN);

	if (3 * m < n)
	{
		beta_down = 0.01f;
	}
	else
	{
		beta_down = quantile_function(beta_ridge, used_m, TOPO_L1_PARA_DOWN);
	}

	L1_para = square_x_down * beta_down;
	memset(square_x, 0, sizeof(*square_x) * TOPO_METER_NUM);

	return L1_para;
}

static void deTwins(topo_huberfun_para_t* para, FP64* repenalty)
{
	FP64* X = para->X;
	FP64* meter_one = para->meter_one;
	FP64* meter_two = para->meter_two;
	FP64 rho = 0.0f;
	FP64 sum1 = 0.0f;
	FP64 sum2 = 0.0f;
	INT16U* used_id = para->used_id;
	INT16U m = para->matrixpara->column1 - 1;
	INT16U n = para->matrixpara->row1;
	INT16U i, j, k;

	memset(meter_one, 0, sizeof(*meter_one) * TOPO_SAMPLE_NUM);
	memset(meter_two, 0, sizeof(*meter_two) * TOPO_SAMPLE_NUM);

	for (i = 0; i < m - 1; i++)
	{
		for (k = 0; k < n; k++)
		{
			meter_one[k] = X[k * TOPO_METER_NUM + used_id[i]];
		}
		for (j = i + 1; j < m; j++)
		{
			for (k = 0; k < n; k++)
			{
				meter_two[k] = X[k * TOPO_METER_NUM + used_id[j]];
			}
			rho = rhofunction(meter_one, meter_two, n);
			sum1 = 0.0;
			sum2 = 0.0;

			if (rho > 0.85f)
			{
				for (k = 0; k < n; k++)
				{
					sum1 += meter_one[k];
					sum2 += meter_two[k];
				}
				if (sum1 > sum2)
				{
					repenalty[used_id[j]] = 1000.0f;
				}
				else
				{
					repenalty[used_id[j]] = 1000.0f;
				}
			}
		}
	}
}

/* ************************************************************************
   huber求解子函数
   函数功能：huber求系数
   输入：结构体变量para
   输出：子表误差系数resultArr[]
************************************************************************ */
static void huberfunction(topo_huberfun_para_t* para, FP64* resultArr)
{
	/* 展开参数 */
	FP64* X = para->X;
	FP64* Y = para->Y;
	topo_matrix_para_t* matrix_para = para->matrixpara;
	FP64* lambda = para->lambda;
	FP64* weight = para->weight;
	FP64* XTX = para->XTX;		// x^T * w * x + lambda * E	 
	FP64* XTX_inv = XTX;		// inv(x^T * w * x + lambda * E)   复用XTX
	INT16U i, j, k;
	INT16U used_num = para->used_num;
	INT16U* used_id = para->used_id;

	memset(XTX, 0, sizeof(*XTX) * TOPO_METER_NUM * TOPO_METER_NUM);

	for (i = 0; i < used_num; i++)
	{
		for (j = 0; j < used_num; j++)
		{
			for (k = 0; k < matrix_para->row1; k++)
			{
				XTX[used_id[i] * TOPO_METER_NUM + used_id[j]] += X[k * TOPO_METER_NUM + used_id[i]] * X[k * TOPO_METER_NUM + used_id[j]] * weight[k];
			}
		}
	}

	for (i = 0; i < matrix_para->column1; i++)
	{
		XTX[used_id[i] * TOPO_METER_NUM + used_id[i]] += lambda[used_id[i]];
	}

	matrix_inverse_storage(XTX_inv, para);
	for (i = 0; i < used_num; i++)
	{
		for (j = 0; j < used_num; j++)
		{
			for (k = 0; k < matrix_para->row1; k++)
			{
				resultArr[used_id[i]] += XTX_inv[used_id[i] * TOPO_METER_NUM + used_id[j]] * X[k * TOPO_METER_NUM + used_id[j]] * weight[k] * Y[k];		// inv(X^T * w * X + lambda * E) * X^T * w * y
			}
		}
	}
}

/* ***********************************************************************
   huber回归子函数
   函数功能：huber回归求解误差系数
   输入：结构体变量data,result,cal_para
   输出：更新后的结构体变量data,result,cal_para
************************************************************************ */
static void huber_regression(topo_data_t* data, topo_result_t* result, topo_cal_para_t* cal_para)
{
#define BETA_THRESHOLD1    0.001f		// 设置beta阈值
#define HUBER_OR_RIDGE     0.0f			// 设置huber还是ridge算法，0为ridge，其它即为huber
#define CYCLE_MAX           7			// 设置最大迭代次数
#define STEP_MAX            2			// 设置最大计算次数
#define HUBER_BREAK_RES    0.001f		// 设置拟合阈值

	FP64 L1_adap = 0.0f;
	FP64* beta = result->beta;
	FP64* fit = cal_para->fit;
	FP64* weight = cal_para->weight;
	FP64* repenalty = cal_para->repenalty;			// 子表和线损的相关性惩罚 
	FP64* beta_last = cal_para->beta_last;			// 上次求解的误差系数beta
	FP64* lambda = cal_para->lambda;			// L1 + L2 + 相关性惩罚的组合
	FP64* residual = cal_para->residual;			// 拟合误差
	FP64* residual_1 = cal_para->residual1;			// 拟合误差
	FP64* beta_steps = cal_para->beta_steps;
	FP64* square_x = repenalty;						// 协方差主对角线，复用repenalty，后续清0
	FP64 outlier_value = 0.0f;						// 异常拟合误差
	FP64 res = 0.0f;								// 均方误差
	FP64 sum_repenalty = 0.0f;
	INT16U meter_num = cal_para->used_num;
	INT16U sample_num = data->used_sample_num;
	INT16U cycle;									// 迭代次数
	INT16U steps;
	INT16U meter_num_steps = 0;
	INT16U i, j;

	topo_matrix_para_t matrixpara = { 0 };
	matrixpara.row1 = sample_num;
	matrixpara.column1 = cal_para->used_num + 1;

	/* huber参数填写 */
	topo_huberfun_para_t huber_para;
	huber_para.X = *(data->child_meter);
	huber_para.Y = cal_para->parent_meter;
	huber_para.meter_one = residual;		    // 复用residual，用完清0
	huber_para.meter_two = residual_1;                  // 复用residual1，用完清0
	huber_para.matrixpara = &matrixpara;
	huber_para.lambda = lambda;
	huber_para.weight = weight;
	huber_para.square_x = square_x;
	huber_para.XTX = *(cal_para->XTX);
	huber_para.U = *(cal_para->U);
	huber_para.used_id = cal_para->used_index_steps;  // 一开始的id是当前层级的数据标号
	huber_para.used_num = cal_para->used_num + 1;
	huber_para.used_meter_num_ori = data->used_meter_num;

#ifdef DEBUG
	printf("Used_Meter_Num: %d\n", meter_num);     // 输出计算实际使用的表数和数据点数
	printf("Used_Sample_Num: %d\n", sample_num);
#endif

	memset(square_x, 0, sizeof(*square_x) * TOPO_METER_NUM);
	memset(beta_steps, 0, sizeof(*beta_steps) * TOPO_METER_NUM);
	for (i = 0; i < meter_num; i++)
	{
		for (j = 0; j < sample_num; j++)
		{
			square_x[i] += data->child_meter[j][cal_para->used_index[i]] * data->child_meter[j][cal_para->used_index[i]];
		}
	}

	memset(cal_para->used_index_steps, 0, sizeof(cal_para->used_index_steps[0]) * TOPO_METER_NUM);
	for (i = 0; i < meter_num; i++)
	{
		cal_para->used_index_steps[i] = cal_para->used_index[i];		// 一开始的id是当前层级的数据标号
	}
	cal_para->used_index_steps[meter_num] = data->used_meter_num;

	for (steps = 0; steps < STEP_MAX; steps++)
	{
		memset(lambda, 0, sizeof(*lambda) * TOPO_METER_NUM);
		memset(weight, 0, sizeof(*weight) * TOPO_SAMPLE_NUM);

		if (steps > 0)		 // 判断第二步需要计算的标号
		{
			meter_num_steps = 0;
			memset(cal_para->used_index_steps, 0, sizeof(cal_para->used_index_steps[0]) * TOPO_METER_NUM);
			for (i = 0; i < meter_num; i++)
			{
				if (beta[cal_para->used_index[i]] > 0.8f)
				{
					beta_steps[cal_para->used_index[i]] = beta[cal_para->used_index[i]];
					for (j = 0; j < sample_num; j++)
					{
						cal_para->parent_meter[j] -= data->child_meter[j][cal_para->used_index[i]];
					}
				}
				else if (beta[cal_para->used_index[i]] < 0.02f)
				{
					beta_steps[cal_para->used_index[i]] = beta[cal_para->used_index[i]];
				}
				else
				{
					cal_para->used_index_steps[meter_num_steps] = cal_para->used_index[i];
					meter_num_steps++;
				}
			}

			if (meter_num_steps < 1)
			{
				break;
			}
			else
			{
				cal_para->used_index_steps[meter_num_steps] = data->used_meter_num;
				meter_num = meter_num_steps;
				matrixpara.column1 = meter_num + 1;
				huber_para.used_num = meter_num + 1;
			}
		}

		for (i = 0; i < meter_num; i++)
		{
			lambda[cal_para->used_index_steps[i]] = TOPO_L2;		// 初始化约束项，L2
		}
		lambda[data->used_meter_num] = TOPO_L2;						// 对常数项不施加L1和相关性惩罚，仅施加L2惩罚

		for (j = 0; j < sample_num; j++)
		{
			weight[j] = 1;											// 初始化样本权重
			data->child_meter[j][data->used_meter_num] = 1;			// 设置常数项为1
		}
		memset(beta, 0, sizeof(*beta) * TOPO_METER_NUM);
		huberfunction(&huber_para, beta);							// 求得初始解

		memset(beta_last, 0, sizeof(*beta_last) * TOPO_METER_NUM);
		for (i = 0; i < meter_num + 1; i++)
		{
			beta_last[cal_para->used_index_steps[i]] = beta[cal_para->used_index_steps[i]];
		}
		if (0 == steps)
		{
			L1_adap = L1adaptive(&huber_para, beta);				// 自适应L1参数
#ifdef DEBUG
			printf("adaptive para is : %f\n", L1_adap);
#endif
			memset(repenalty, 0, sizeof(*repenalty) * TOPO_METER_NUM);
			deTwins(&huber_para, repenalty);						// 去相关性
		}

		for (cycle = 0; cycle < CYCLE_MAX; cycle++)
		{
			memset(beta, 0, sizeof(*beta) * TOPO_METER_NUM);

			/* 根据上次求解的beta值对L1惩罚系数进行更新 */
			for (i = 0; i < meter_num; i++)
			{
				if (fabs(beta_last[cal_para->used_index_steps[i]]) < BETA_THRESHOLD1)
				{
					lambda[cal_para->used_index_steps[i]] = 1000.0f;
				}
				else
				{
					lambda[cal_para->used_index_steps[i]] = (FP64)(1.0 / fabs(beta_last[cal_para->used_index_steps[i]]));		//L1部分 分两种情况软分离
				}
			}
			for (i = 0; i < meter_num; i++)
			{
				lambda[cal_para->used_index_steps[i]] = TOPO_L2 + L1_adap * lambda[cal_para->used_index_steps[i]] + TOPO_RELA_COEF * repenalty[cal_para->used_index_steps[i]];		// L2、L1、相关性三部分的组合
			}

			/* 根据上次求解的beta值对weight进行更新 */
			/* 计算拟合线损值 fit */
			memset(fit, 0, sizeof(*fit) * TOPO_SAMPLE_NUM);
			for (i = 0; i < sample_num; i++)
			{
				for (j = 0; j < meter_num; j++)
				{
					fit[i] += data->child_meter[i][cal_para->used_index_steps[j]] * beta_last[cal_para->used_index_steps[j]];
				}
				fit[i] += beta_last[data->used_meter_num];
			}

			/* 计算拟合残差 residual */
			memset(residual, 0, sizeof(residual_1[0]) * TOPO_SAMPLE_NUM);
			memset(residual_1, 0, sizeof(residual_1[0]) * TOPO_SAMPLE_NUM);
			for (i = 0; i < sample_num; i++)
			{
				residual[i] = (FP64)fabs(fit[i] - cal_para->parent_meter[i]);
				residual_1[i] = residual[i];
			}

			outlier_value = quantile_function(residual_1, sample_num, 1.0f - HUBER_OR_RIDGE);

			/* 计算样本权重 */
			for (i = 0; i < sample_num; i++)
			{
				if (residual[i] > outlier_value)				 // outlier_value为residual的九分位数
				{
					weight[i] = outlier_value / residual[i];	 // 样本权重 拟合线损值与实际线损值偏差越大，权重越小
					if (weight[i] < 0.5f)
					{
						weight[i] = 0.0f;
					}
				}
				else
				{
					weight[i] = 1.0f;
				}
			}

			huberfunction(&huber_para, beta);		// 循环收缩，即增添施加L1、相关性惩罚

			res = 0;								// 前后两次求解系数的拟差平方
			for (i = 0; i < meter_num + 1; i++)
			{
				res += (beta[cal_para->used_index_steps[i]] - beta_last[cal_para->used_index_steps[i]]) * (beta[cal_para->used_index_steps[i]] - beta_last[cal_para->used_index_steps[i]]);
			}
			res = (FP64)sqrt(res);

			if (res < HUBER_BREAK_RES || CYCLE_MAX - 1 == cycle)
			{
				break;
			}
			for (i = 0; i < meter_num + 1; i++)
			{
				beta_last[cal_para->used_index_steps[i]] = beta[cal_para->used_index_steps[i]];
			}
		}
		memset(residual_1, 0, sizeof(*residual_1) * TOPO_SAMPLE_NUM);
	}

	if (meter_num_steps > 0)
	{
		for (i = 0; i < meter_num + 1; i++)
		{
			beta_steps[cal_para->used_index_steps[i]] = beta[cal_para->used_index_steps[i]];
		}
	}

	for (i = 0; i < TOPO_METER_NUM; i++)
	{
		beta[i] = beta_steps[i];
	}

#undef BETA_THRESHOLD1		// 终止宏的定义域
#undef HUBER_OR_RIDGE 
#undef CYCLE_MAX      
#undef HUBER_BREAK_RES

}

/* ***********************************************************
两个数组若有交集则第一个数组取并集，第二个数组置空
函数功能：两个数组若有交集则第一个数组取并集，第二个数组置空
输入：两个数组X,Y
输出：更新数组X,Y，返回1表示有交集，返回0表示没有交集
*********************************************************** */
static INT8U if_intersect_union(INT16U X[], INT16U Y[], INT16U N)
{
	INT16U i, j, k, p, q;
	INT16U Z[TOPO_METER_NUM] = { 0 };
	INT8U belong_interect = 0;

	k = 0;		// 交集个数
	for (i = 0; i < N; i++)
	{
		for (j = 0; j < N; j++)
		{
			if (X[i] == Y[j] && X[i] != 0)
			{
				Z[k] = X[i];
				k++;		// 记录交集个数
				break;
			}
		}
	}

	if (k > 0)		// 若2个数组有交集
	{
		q = 0;
		for (i = 0; i < N; i++)		//先将X的左边填充X里非交集里的元素
		{
			belong_interect = 0;
			for (p = 0; p < k; p++)
			{
				if (X[i] == Z[p])
				{
					belong_interect = 1;
					break;
				}
			}
			if (belong_interect != 1 && X[i] != 0)
			{
				X[q] = X[i];
				q++;
			}
		}

		for (i = 0; i < N; i++)		// 继续在X里填充Y里非交集里的元素
		{
			belong_interect = 0;
			for (p = 0; p < k; p++)
			{
				if (Y[i] == Z[p])
				{
					belong_interect = 1;
					break;
				}
			}
			if (belong_interect != 1 && Y[i] != 0)
			{
				X[q] = Y[i];
				q++;
			}
		}

		for (i = 0; i < k; i++)		// 最后在X里填充交集里的元素
		{
			X[i + q] = Z[i];
		}

		memset(Y, 0, N);		  // 将Y清空，全部置零

		return 1;				 // 若存在交集，则函数返回1
	}

	return 0;					// 若不存在交集，则函数返回0
}

/* ***********************************************************
电压聚类子函数
函数功能：通过电压给电表聚类，降低计算维度
输入：结构体变量data
输出：更新结构体变量data，填充child_meter_after_cluster
*********************************************************** */
INT16U cluster[TOPO_METER_NUM - TOPO_THRIPLE_NUM][TOPO_METER_NUM - TOPO_THRIPLE_NUM];  // 皮尔逊相似性矩阵
static void voltage_cluster(topo_data_t* data, topo_voltage_t* voltage, topo_cal_para_t* cal_para)
{
	FP64* voltage_meter1 = cal_para->beta_last;				// 抽出单个表电压
	FP64* voltage_meter2 = cal_para->beta_steps;			// 抽出单个表电压
	INT16U* device_type = data->device_type;				// 设备类型号
	FP64* child_meter_ori = *(data->child_meter_ori);		// 初始功率
	INT16U used_meter_num = data->used_meter_num;			// 设备数
	INT16U used_sample_num = data->used_voltage_num;		// 数据点数
	FP64* child_meter = *(data->child_meter);				// 聚类后的功率（聚类后单相表簇放在左边，LTU、三相表放在右边）

	INT16U* ind_original2cluster = data->ind_original2cluster;		// 电表聚类后与原始序号的对应关系（所有设备）
	INT16U* cluster1 = cal_para->used_index;						// 抽出单个簇
	INT16U* cluster2 = cal_para->used_index_steps;					// 抽出单个簇
	INT16U single_meter_num = 0;									// 单相表个数
	INT8U change_or_not = 0;
	INT8U empty_or_not = 0;
	FP64* rho = *(cal_para->XTX);
	INT16U i, j, k, m;
	INT16U id = 0;
	INT16U box_cluster_num = 0;
	INT16U not_single_meter_num = 0;								// 非单相表个数（包括终端、LTU、三相表）
	INT16U device_num_after_cluster = 0;							// 聚类后的设备数（单相表簇、LTU、三相表）
	INT16U* ind_SINGLE = voltage->ind_SINGLE;						// 单相表在所有设备中的位置序列
	INT16U* ind_TRIPLE = voltage->ind_TRIPLE;						// 三相表在所有设备中的位置序列

	memset(voltage_meter1, 0, sizeof(*voltage_meter1) * TOPO_SAMPLE_NUM);
	memset(voltage_meter2, 0, sizeof(*voltage_meter2) * TOPO_SAMPLE_NUM);
	memset(cluster1, 0, sizeof(*cluster1) * TOPO_METER_NUM);
	memset(cluster2, 0, sizeof(*cluster2) * TOPO_METER_NUM);

	memset(cluster, 0, sizeof(cluster));
	memset(child_meter, 0, sizeof(*child_meter) * TOPO_SAMPLE_NUM * TOPO_METER_NUM);
	memset(ind_original2cluster, 0, sizeof(ind_original2cluster));
	memset(ind_SINGLE, 0, sizeof(*ind_SINGLE) * (TOPO_METER_NUM - TOPO_THRIPLE_NUM));
	memset(ind_TRIPLE, 0, sizeof(*ind_TRIPLE) * TOPO_THRIPLE_NUM);

	cal_para->meter_num_ori = data->used_meter_num;		// 记录聚类前的初始设备数

	// 循环device_type，填充ind_SINGLE和ind_TRIPLE
	k = 0;
	m = 0;
	for (i = 0; i < data->used_meter_num; i++)
	{
		if (0 != device_type[i])
		{
			ind_TRIPLE[k] = i;
			k++;
		}
		else
		{
			ind_SINGLE[m] = i;
			m++;
		}
	}

	single_meter_num = m;		// 单相表个数
	for (i = 0; i < single_meter_num; i++)
	{
		for (j = 0; j < used_sample_num; j++)
		{
			voltage_meter1[j] = voltage->voltage_SINGLE[j][i];		// 抽出单个表i的电压
		}
		for (j = i; j < single_meter_num; j++)						// 从i开始，i与i做相似度为1
		{
			for (k = 0; k < used_sample_num; k++)
			{
				voltage_meter2[k] = voltage->voltage_SINGLE[k][j];	// 抽出单个表i的电压
			}
			rho[i * TOPO_METER_NUM + j] = rhofunction(voltage_meter1, voltage_meter2, used_sample_num);
		}
	}

	// memset(rho, 0, sizeof(*rho) * TOPO_METER_NUM * TOPO_METER_NUM);	// 测试下面1425行用
	for (i = 0; i < single_meter_num; i++)							// 电压相似度>0.98的聚成一簇
	{
		k = 0;
		for (j = i; j < single_meter_num; j++)
		{
			if (rho[i * TOPO_METER_NUM + j] > TOPO_VOLTAGE_CLUSTER_LIMIT || i == j)	// 加i == j 是为了防止落下零散一个的情况（某个表电压全为0）可测所有单相表电压数据全为0
			{
				cluster[i][k] = (j + 1);		 // +1的目的是为了让表号不存在0
				k++;
			}
		}
	}

	for (i = 0; i < single_meter_num; i++)		// 循环这些簇，有交集的并到第一簇里
	{
		for (k = 0; k < single_meter_num; k++)
		{
			cluster1[k] = (cluster[i][k]);		// 抽出单个簇
		}
		for (j = 0; j < single_meter_num; j++)
		{
			if (j == i)
			{
				continue;
			}

			for (k = 0; k < single_meter_num; k++)
			{
				cluster2[k] = (cluster[j][k]);	// 抽出单个簇
			}

			change_or_not = if_intersect_union(cluster1, cluster2, single_meter_num);		// cluster1和cluster2有交集则cluster1取并集，cluster2置空

			if (1 == change_or_not)
			{
				for (m = 0; m < single_meter_num; m++)		// 将cluster1赋回给cluster第i行
				{
					cluster[i][m] = cluster1[m];
				}

				for (m = 0; m < single_meter_num; m++)	   // cluster第j行元素置0 
				{
					cluster[j][m] = 0;
				}
			}
		}
	}

	for (i = 0; i < single_meter_num; i++)				 // 循环这些簇，只保留非空的簇，cluster将非空簇放到前面
	{
		empty_or_not = 0;
		for (j = 0; j < single_meter_num; j++)
		{
			if (cluster[i][j] != 0)
			{
				empty_or_not = 1;
				break;
			}
		}

		if (empty_or_not == 1)
		{
			for (j = 0; j < single_meter_num; j++)
			{
				cluster[box_cluster_num][j] = cluster[i][j];
			}
			box_cluster_num++;								// 最终box_cluster_num为非空簇的个数，即聚集成多少簇
		}
	}

	for (i = box_cluster_num; i < single_meter_num; i++)	// 将后面的簇全置零
	{
		for (j = 0; j < single_meter_num; j++)
		{
			cluster[i][j] = 0;
		}
	}

	for (i = 0; i < box_cluster_num; i++)		// 循环cluster的序号，不是0的话利用ind_SINGLE映射到addr
	{
		for (j = 0; j < single_meter_num; j++)
		{
			id = (cluster[i][j]);
			if (id != 0)
			{
				cluster[i][j] = ind_SINGLE[id - 1] + 1;
			}
		}
	}

	for (i = 0; i < box_cluster_num; i++)
	{
		for (j = 0; j < single_meter_num; j++)
		{
			id = (cluster[i][j]);
			if (id != 0)
			{
				ind_original2cluster[id - 1] = i + 1;
			}
		}
	}

	// ind_original2cluster里面没填行号的位置（数值为0）从box_cluster_num + 1开始填（因为非单相设备的功率放在了child_meter右边）
	k = 0;
	for (i = 0; i < data->used_meter_num; i++)
	{
		if (0 == ind_original2cluster[i])
		{
			ind_original2cluster[i] = box_cluster_num + 1 + k;
			k++;
		}
	}

	// ind_original2cluster所有位置的数-1，这样全部从0开始
	for (i = 0; i < data->used_meter_num; i++)
	{
		ind_original2cluster[i] = ind_original2cluster[i] - 1;
	}

	// 将簇的功率加和，放在child_meter左边
	for (i = 0; i < box_cluster_num; i++)
	{
		for (k = 0; k < single_meter_num; k++)
		{
			id = (cluster[i][k]);
			if (id != 0)
			{
				for (j = 0; j < TOPO_SAMPLE_NUM; j++)
				{
					child_meter[j * TOPO_METER_NUM + i] = child_meter[j * TOPO_METER_NUM + i] + child_meter_ori[j * TOPO_METER_NUM + id - 1];
				}
			}
		}
	}

	// 找除单相表外的设备个数
	for (j = 0; j < data->used_meter_num; j++)
	{
		if (device_type[j] != 0)
		{
			not_single_meter_num++;
		}
	}

	device_num_after_cluster = not_single_meter_num + box_cluster_num;		// 聚类后的设备个数（单相表簇数加LTU、三相表）

	// 将除单相表外的设备的功率放在child_meter右边
	INT16U box_cluster_num_next = 0;
	for (j = 0; j < data->used_meter_num; j++)
	{
		if (device_type[j] != 0)
		{
			for (k = 0; k < TOPO_SAMPLE_NUM; k++)
			{
				child_meter[k * TOPO_METER_NUM + box_cluster_num + box_cluster_num_next] = child_meter_ori[k * TOPO_METER_NUM + j];
			}
			box_cluster_num_next++;
		}
	}

	data->used_meter_num = device_num_after_cluster;		// 将data->used_meter_num更新为聚类后的个数

	memset(voltage_meter1, 0, sizeof(*voltage_meter1) * TOPO_SAMPLE_NUM);
	memset(voltage_meter2, 0, sizeof(*voltage_meter2) * TOPO_SAMPLE_NUM);
	memset(cluster1, 0, sizeof(*cluster1) * TOPO_METER_NUM);
	memset(cluster2, 0, sizeof(*cluster2) * TOPO_METER_NUM);
}

#ifdef DEBUG
FP64 beta_all[1000][200] = { 0.0 };
FILE* fp = NULL;
#endif

/* ***********************************************************
拓扑梳理子函数
函数功能：根据功率数据获得父子对应关系，并返回未挂上的设备序号
输入：结构体变量data result cal_para
输出：更新结构体变量result，填充 result.topo_result 和 result.unarrange
*********************************************************** */
INT16U topo_calculate(topo_data_t* data, topo_result_t* result, topo_cal_para_t* cal_para)
{
	memset(result, 0, sizeof(topo_result_t));		// 初始化结构体变量topo_result_t

	INT8U IsFromTerminal = 1;
	INT8U IsOnlyFirstLine = 1;
	INT16U i, j, k, p = 0;
	INT16U parent_num = 0;
	INT16U used_child_num = 0;
	INT16U zongbiao_no = 0;
	INT16U layer_num = 0;
	INT16U* unarange = &(cal_para->used_index[0]);			// 未上挂表个数
	INT16U* find_belong = unarange;
	INT16U unarange_num = 0;
	INT16U* store_child = &(cal_para->used_index_steps[0]);
	INT16U* store_parent = &(data->PT[0]);
	INT16U parent_ind_ori = 0;
	INT16U FirstLineIsNotChild = 0;

	FP64 max_weight = 0.0f;
	FP64* max_beta_vector = &(cal_para->max_beta[0]);		// 存储最大系数
	FP64* max_weight_vector = &(cal_para->max_weight[0]);	// 存储最大权重系数
	FP64* error_one = &(cal_para->fit[0]);					// 子节点加和，复用
	FP64 error_rate_one = 0.0f;
	FP64 sum_meter[TOPO_METER_NUM] = { 0.0f };				// 每个设备的功率之和

#ifdef DEBUG
	fp = fopen("beta_all.txt", "w+");
#endif

	/* 计算每个设备的功率之和，用于将used_parent_meter_index按用电量从大到小排列 */
	for (i = 0; i < data->used_meter_num; i++)
	{
		for (j = 0; j < data->used_sample_num; j++)
		{
			sum_meter[i] = sum_meter[i] + data->child_meter[j][i];
		}
	}

	/* 找出终端 */
	for (i = 0; i < cal_para->meter_num_ori; i++)
	{
		if (data->device_type[i] == TOPO_MASTER_METER)
		{
			zongbiao_no = data->ind_original2cluster[i];		// 总表在聚类后的位置序号
			break;
		}
	}

	/* 找出一级出线开关 */
	for (i = 0; i < cal_para->meter_num_ori; i++)
	{
		if (data->device_type[i] == TOPO_FIRST_BRANCH)
		{
			// layer_num += 1;					// 自添加部分
			IsFromTerminal = 0;				// 有一级出线则IsFromTerminal = 0
			result->topo_result[data->ind_original2cluster[i]][0] = data->ind_original2cluster[i];
			result->topo_result[data->ind_original2cluster[i]][1] = zongbiao_no;
			result->topo_result[data->ind_original2cluster[i]][2] = layer_num;
			
		}

		if (data->device_type[i] == TOPO_NORMAL_LTU)		// 判断是否只有一级出线（不严谨，当没有设置一级出线且只有一级开关  则不成立）要求一级出线和普通开关的设备类型号必须区分开
		{
			IsOnlyFirstLine = 0;			// 有普通LTU则IsOnlyFirstLine = 0
			// layer_num += 1;                 // 自添加部分
		}
	}

	/* 找出可作为父节点在聚类后的序号 */
	memset(cal_para->used_parent_meter_index, 0, sizeof(cal_para->used_parent_meter_index[0]) * TOPO_PARENTMAXNUM);
	for (i = 0; i < cal_para->meter_num_ori; i++)
	{
		if (data->device_type[i] != TOPO_METER_SINGLE && data->device_type[i] != TOPO_METER_THREE)
		{
			if (IsFromTerminal == 0 && (data->device_type[i] == TOPO_MASTER_METER))  // 如果有一级出线则跳过终端（有一级出线则终端不作为父节点）
			{
				continue;
			}

			cal_para->used_parent_meter_index[parent_num] = data->ind_original2cluster[i];
			parent_num++;
		}
	}

	/* 循环父节点进行拓扑梳理 */
	memset(max_beta_vector, 0, sizeof(*max_beta_vector) * TOPO_METER_NUM);
	memset(max_weight_vector, 0, sizeof(*max_weight_vector) * TOPO_METER_NUM);

	for (i = 0; i < parent_num; i++)
	{
		memset(cal_para->used_index, 0, sizeof(cal_para->used_index[0]) * TOPO_METER_NUM);
		used_child_num = 0;
		for (j = 0; j < data->used_sample_num; j++)
		{
			cal_para->parent_meter[j] = data->child_meter[j][cal_para->used_parent_meter_index[i]];		// 父节点数据
		}

		for (k = 0; k < data->used_meter_num; k++)
		{
			if (sum_meter[k] < sum_meter[cal_para->used_parent_meter_index[i]])		 // 找出可作为子节点的编号
			{
				if (IsFromTerminal == 0)		// IsFromTerminal == 0 (如果不是从终端开始梳理，则一级出线不会作为子节点)  IsOnlyFirstLine == 1
				{
					FirstLineIsNotChild = 0;
					for (p = 0; p < data->meter_num_ori; p++)
					{
						if (data->ind_original2cluster[p] == k && data->device_type[p] == TOPO_FIRST_BRANCH)  //说明k对应的设备是个一级出线，k不能作为子节点去计算
						{
							FirstLineIsNotChild = 1;
							break;
						}
					}

					if (FirstLineIsNotChild == 1)
					{
						continue;
					}
				}

				cal_para->used_index[used_child_num] = k;
				used_child_num++;
			}
		}

		cal_para->used_num = used_child_num;

#ifdef DEBUG
		printf("----第 %d 个开关执行计算----\n", i);
#endif
		huber_regression(data, result, cal_para);      // huber求解，更新result和cal_para

#ifdef DEBUG
		for (INT16U q = 0; q < used_child_num; q++)
		{
			beta_all[cal_para->used_index[q]][i] = result->beta[cal_para->used_index[q]];
		}
#endif

		/* 仅有一级出线则按照系数最大原则 */
		if (IsOnlyFirstLine == 1)					// 仅有终端也走这条逻辑
		{
			for (j = 0; j < used_child_num; j++)
			{
				if (result->beta[cal_para->used_index[j]] > TOPO_MAX_BETA_LIMIT && result->beta[cal_para->used_index[j]] > max_beta_vector[cal_para->used_index[j]])
				{
					max_beta_vector[cal_para->used_index[j]] = result->beta[cal_para->used_index[j]];
					result->topo_result[cal_para->used_index[j]][0] = cal_para->used_index[j];
					result->topo_result[cal_para->used_index[j]][1] = cal_para->used_parent_meter_index[i];
					result->topo_result[cal_para->used_index[j]][2] = layer_num;
				}
			}
		}

		/* 否则按照阈值筛选且占父节点权重最大 */
		else
		{
			for (j = 0; j < used_child_num; j++)
			{
				max_weight = sum_meter[cal_para->used_index[j]] / sum_meter[cal_para->used_parent_meter_index[i]];
				if (result->beta[cal_para->used_index[j]] > TOPO_JUDGE_LIMIT && max_weight > max_beta_vector[cal_para->used_index[j]])
				{
					max_weight_vector[cal_para->used_index[j]] = max_weight;
					result->topo_result[cal_para->used_index[j]][0] = cal_para->used_index[j];
					result->topo_result[cal_para->used_index[j]][1] = cal_para->used_parent_meter_index[i];
					result->topo_result[cal_para->used_index[j]][2] = layer_num;
				}
			}
		}
	}

#ifdef DEBUG
	for (INT16U p = 0; p < 626; p++)
	{
		for (INT16U q = 0; q < 126; q++)
		{
			fprintf(fp, "%.4f ", (beta_all[p][q]));
		}
		fprintf(fp, "\n");
	}
	fclose(fp);
#endif
#ifdef DEBUG
	fp = fopen("result_all_C.txt", "w+");
	for (INT16U p = 0; p < 626; p++)
	{
		for (INT16U q = 0; q < 2; q++)
		{
			fprintf(fp, "%d ", result->topo_result[p][q]);
		}
		fprintf(fp, "\n");
	}
	fclose(fp);
#endif

	/* 各级开关比较齐全的情况加入验证环节 */
	if (IsOnlyFirstLine == 0)    //有普通LTU 
	{
		for (i = 0; i < parent_num; i++)
		{
			error_rate_one = 0.0f;
			memset(error_one, 0, sizeof(*error_one) * TOPO_SAMPLE_NUM);
			for (j = 0; j < data->used_sample_num; j++)
			{
				cal_para->parent_meter[j] = data->child_meter[j][cal_para->used_parent_meter_index[i]];//父节点数据		
				error_one[j] = cal_para->parent_meter[j];
			}
			for (j = 0; j < data->used_meter_num; j++)
			{
				if (result->topo_result[j][1] == cal_para->used_parent_meter_index[i] && result->topo_result[j][0] != result->topo_result[j][1]) // 找到其子节点
				{
					for (k = 0; k < data->used_sample_num; k++)
					{
						error_one[k] -= data->child_meter[k][result->topo_result[j][0]];
					}
				}
			}
			for (j = 0; j < data->used_sample_num; j++)
			{
				error_rate_one += fabs(error_one[j]) / (cal_para->parent_meter[j] + 0.000001f);
			}
			error_rate_one = error_rate_one / data->used_sample_num;
			if (error_rate_one > TOPO_FIT_JUDGE_MAE)
			{
#ifdef DEBUG
				printf("拟合验证不通过，删除父节点 %d 的所有结果\n", cal_para->used_parent_meter_index[i]);     //
#endif
				for (j = 0; j < data->used_meter_num; j++)
				{
					if (result->topo_result[j][1] == cal_para->used_parent_meter_index[i] && result->topo_result[j][0] != result->topo_result[j][1])  // 结果置空
					{
						result->topo_result[j][0] = 0;
						result->topo_result[j][1] = 0;
						result->topo_result[j][2] = 0;
					}
				}
			}
		}
	}

	// 如果从一级出线开始梳理，则在恢复原始序号前把聚类后终端序号放在父节点中
	if (IsFromTerminal == 0)
	{
		for (i = 0; i < cal_para->meter_num_ori; i++)   // 找出终端
		{
			if (data->device_type[i] == TOPO_MASTER_METER)
			{
				cal_para->used_parent_meter_index[parent_num] = data->ind_original2cluster[i];
				parent_num += 1;
				break;
			}
		}
	}

	/* 拓扑结果恢复到原始序号 */
	memset(store_child, 0, sizeof(*store_child) * TOPO_METER_NUM);
	memset(store_parent, 0, sizeof(*store_parent) * TOPO_METER_NUM);
	memset(find_belong, 0, sizeof(*find_belong) * TOPO_METER_NUM);
	for (j = 0; j < data->used_meter_num; j++)
	{
		store_child[j] = result->topo_result[j][0];
		store_parent[j] = result->topo_result[j][1];
	}
	memset(result, 0, sizeof(topo_result_t));

	for (i = 0; i < parent_num; i++)
	{
		parent_ind_ori = 0;
		for (j = 0; j < cal_para->meter_num_ori; j++)  // 循环原始设备数
		{
			if (data->ind_original2cluster[j] == cal_para->used_parent_meter_index[i]) // 寻找父节点对应的原始序号
			{
				parent_ind_ori = j;
				break;
			}
		}
		for (j = 0; j < data->used_meter_num; j++)   // 循环聚类后的表数
		{
			if (store_parent[j] == cal_para->used_parent_meter_index[i] && store_parent[j] != store_child[j]) // 找到聚类后的子节点序号
			{
				for (k = 0; k < cal_para->meter_num_ori; k++) // 循环原始设备数
				{
					if (data->ind_original2cluster[k] == store_child[j]) // 寻找子节点对应的原始序号
					{
						find_belong[k] = 1;
						result->topo_result[k][0] = k;
						result->topo_result[k][1] = parent_ind_ori;  // 原程序
						result->topo_result[k][2] = 0;
						 
						
					}
				}
			}
		}
	}


	/* 确定未挂上的表序号 */
	for (i = 0; i < cal_para->meter_num_ori; i++)
	{
		if (find_belong[i] == 0 && data->device_type[i] != TOPO_MASTER_METER)
		{
			unarange[unarange_num] = i;
			unarange_num++;
		}
	}
	for (i = unarange_num; i < cal_para->meter_num_ori; i++)
	{
		unarange[i] = 0;
	}
	result->unarange = unarange;
	result->unarange_num = unarange_num;


	return 0;
}

/*步骤①：根据提供的失败表/开关,区分表和开关，分别处理。
  步骤②：根据标号拉取失败表的单相电压数据，三相表或开关只拿A相电压数据。
  步骤③：遍历所有的开关设备，分别取开关的三相电压数据，与电表电压做皮尔逊系数，只保留最大的值和对应开关标号。
  步骤④：如果下面的皮尔逊高于此皮尔逊，进行更新。
  步骤⑤：最大的皮尔逊高于阈值，则将表挂在对应开关下。*/
  ///***************************************************************************************************///
   //电压跟随方案//
#define BranchNum 3
//FP64 pearson_result[TOPO_METER_NUM] = { 0 };//记录皮尔逊识别值结果

FP64 corr_p[1][BranchNum] = { 0.0f };//记录皮尔逊计算结果
//FP64 voltage_meters_all[TOPO_METER_NUM][TOPO_SAMPLE_NUM] = { 0.0f }; //记录所有电表电压曲线数据
FP64 voltage_failed_meters[TOPO_SAMPLE_NUM] = { 0.0f }; //记录某失败电表电压曲线数据
FP64 voltage_branch[BranchNum][TOPO_SAMPLE_NUM] = { 0.0f }; //记录分支三相电压曲线数据
//FP64 voltage_meters_one[TOPO_SAMPLE_NUM] = { 0.0f }; //记录某只电表电压曲线数据

//FP64 meter_temp[TOPO_SAMPLE_NUM] = { 0.0f };
//FP64 phase_temp[TOPO_SAMPLE_NUM] = { 0.0f };
//INT16U count_failed_meter = 0;//外部传递进来


//步骤①：根据提供的失败表 / 开关, 找到对应标号。
static void Get_Vol_Data_Failed_Meter(topo_voltage_t* voltage, FP64* voltage_failed_meters, INT16U failed_meter_idx)
{
	INT16U i = 0;
	INT16U j = 0;
	INT16U flag = 0;
	//最大表数可根据实际单相表和三相表数据走循环，节约计算资源。
	for (i = 0; i < TOPO_THRIPLE_NUM; i++)
	{
		if (failed_meter_idx == voltage->ind_TRIPLE[i])
		{
			for (j = 0; j < TOPO_VOLTAGE_NUM; j++)
			{
				voltage_failed_meters[j] = voltage->voltage_TRIPLE[j][i][0];
			}
			flag = 1;
			break;
		}
	}
	if (flag != 1)
	{
		for (i = 0; i < TOPO_METER_NUM - TOPO_THRIPLE_NUM; i++)
		{
			if (failed_meter_idx == voltage->ind_SINGLE[i])
			{
				for (j = 0; j < TOPO_VOLTAGE_NUM; j++)
				{
					voltage_failed_meters[j] = voltage->voltage_SINGLE[j][i];
				}
				break;
			}
		}
	}
}
static void Standard(FP64 a[], INT16U n, FP64* s)
{
	FP64 aver = 0.0f;
	FP64 sum = 0.0f, e = 0.0f;
	INT16U i = 0;
	//FP64 fn = 0.0f;

	for (i = 0; i < n; i++)
	{
		sum += (a[i]);
	}

	//fn = (FP64)n*1.0f;

	aver = sum / n;
	for (i = 0; i < n; i++)
		e += (a[i] - aver) * (a[i] - aver);
	e /= n - 1;
	*s = (FP64)sqrt(e);
}


//电压曲线跟随//
static FP64 voltage_follower(topo_data_t* data)
{
	FP64 standard_A = 0.0f, standard_B = 0.0f; //求取数据数组的标准差
	FP64 cox_ab = 0.0f, cos_ab = 0.0f;//a，b两数组的协方差//
	INT16U i = 0, j = 0, p = 0;
	INT16U max_index = 0;
	FP64 max;
	INT16U  phase_result_one = 0;
	//清零操作//
	//memset(voltage_meters_one, 0, sizeof(voltage_meters_one));
	//判断有效数据个数//
	if (data->used_voltage_num < TOPO_SAMPLE_MIN) //有效数据样本大于20个，或有效数据是电表个数的2倍//
		return 0;
	standard_A = 0.0f;
	standard_B = 0.0f;
	for (p = 0; p < BranchNum; p++)
	{
		//for (j = 0; j < data->used_voltage_num; j++)
		//{
		//	meter_temp[j] = voltage_failed_meters[j];
		//	phase_temp[j] = voltage_branch[p][j];
		//}
		//根据线损值与电表示值求相关系数corrcoef_value
		Standard(voltage_failed_meters, data->used_voltage_num, &standard_A);
		Standard(voltage_branch[p], data->used_voltage_num, &standard_B);
		cox(voltage_branch[p], voltage_failed_meters, data->used_voltage_num, &cox_ab);
		if (standard_A * standard_B > 0.000001)
			corr_p[i][p] = cox_ab / (standard_A * standard_B);
		else
		{
			corr_p[i][p] = 0.0f;
		}
	}
	max = corr_p[i][0];
	max_index = 0;
	for (p = 1; p < BranchNum; p++)
	{
		if (corr_p[i][p] > max)
		{
			max = corr_p[i][p];
			max_index = p;
		}
	}
	phase_result_one = max_index + 1;
	return max;
}

//电压跟随方案的相位识别//
//提取存储的电压数据，电压数据与有功功率不同，电压的话三相表是6字节，单相表为2字节！
static void voltage_complement(topo_data_t* data, topo_voltage_t* voltage, topo_result_t* result)
{
	INT16U* failed_meter = result->unarange;//记录失败表索引
	INT16U  count_num = 0;
	INT16U i = 0, j = 0, k = 0, p = 0;
	INT16U failed_meter_idx = 0;
	INT16U add = 0;
	INT16U flag_add = 0;
	FP64 temp_max = 0.0f;
	FP64 now_max = 0.0f;
	INT8U flag = 0;
	INT16U max_ind = 0;
	for (i = 0; i < result->unarange_num; i++)
	{
		max_ind = 0;
		temp_max = 0.0;
		failed_meter_idx = failed_meter[i];// 每一只失败表索引
		if (data->device_type[failed_meter_idx] < 2)//判断是否是表或开关，单独处理
		{
			//成立即是电表，得到电表数据
			memset(voltage_failed_meters, 0, sizeof(voltage_failed_meters));
			Get_Vol_Data_Failed_Meter(voltage, voltage_failed_meters, failed_meter_idx);//得到每一只失败表的电压数据
			//得到开关三相电压数据
			memset(voltage_branch, 0, sizeof(voltage_branch));
			for (j = 0; j < TOPO_THRIPLE_NUM; j++)
			{
				if (data->device_type[voltage->ind_TRIPLE[j]] > 1)
				{
					for (k = 0; k < TOPO_SAMPLE_NUM; k++)
					{
						for (p = 0; p < BranchNum; p++)
						{
							voltage_branch[p][k] = voltage->voltage_TRIPLE[k][j][p];
						}
					}
					//满足条件的求取皮尔逊系数,保留值
					//pearson_result[j] = voltage_follower(data);
					now_max = voltage_follower(data);
					if (now_max > temp_max)
					{
						temp_max = now_max;
						max_ind = j;
					}
				}
			}
			if (temp_max > TOPO_PEARSON_LIMIT)
			{
				//child_to_parent[count_num][0] = voltage->ind_SINGLE[i];
				//child_to_parent[count_num][1] = voltage->ind_TRIPLE[j];
				result->topo_result[failed_meter_idx][0] = failed_meter_idx;
				result->topo_result[failed_meter_idx][1] = voltage->ind_TRIPLE[max_ind];
				count_num = count_num + 1;
			}
		}
		//如果是开关，需要有开关的逻辑判断
		else
		{
			//else逻辑走开关，得到开关A相电压数据（只拿A相数据）
			memset(voltage_failed_meters, 0, sizeof(voltage_failed_meters));
			Get_Vol_Data_Failed_Meter(voltage, voltage_failed_meters, failed_meter_idx);//得到每一只失败表的电压数据
			//除去开关自身，自身皮尔逊系数不求。且必须是开关，不能是三相表。
			//开关只需和A相比较即可

			for (j = 0; j < TOPO_THRIPLE_NUM; j++)
			{
				if ((data->device_type[voltage->ind_TRIPLE[j]] > 1) && (failed_meter_idx != voltage->ind_TRIPLE[j]))
				{
					add = 0;
					flag_add = 0;
					for (k = 0; k < data->used_voltage_num; k++)
					{
						for (p = 0; p < 1; p++)
						{
							voltage_branch[p][k] = voltage->voltage_TRIPLE[k][j][p];
							if (voltage_branch[p][k] < voltage_failed_meters[k])
							{
								add = add + 1;
							}
						}
					}
					if (add < 6)
						flag_add = 1;
					//计算漏的开关和其他开关A相电压曲线是否一个比另一个小。

					//满足条件的求取皮尔逊系数
					//pearson_result[j] = voltage_follower(data);
					now_max = voltage_follower(data);
					if (now_max > temp_max && flag_add == 1)
					{
						temp_max = now_max;
						max_ind = j;
					}
				}
			}
			if (temp_max > TOPO_PEARSON_LIMIT)
			{
				//child_to_parent[count_num][0] = voltage->ind_SINGLE[i];
				//child_to_parent[count_num][1] = voltage->ind_TRIPLE[j];
				result->topo_result[failed_meter_idx][0] = failed_meter_idx;
				result->topo_result[failed_meter_idx][1] = voltage->ind_TRIPLE[max_ind];
				count_num = count_num + 1;
			}
		}
	}
	memset(result->unarange, 0, sizeof(result->unarange[0]) * TOPO_METER_NUM);
	result->unarange_num = 0;
	count_num = 0;
	for (i = 0; i < data->meter_num_ori; i++)
	{
		if (result->topo_result[i][0] == 0 && data->device_type[i] != TOPO_MASTER_METER)
		{
			result->unarange[count_num] = i;
			count_num++;
		}
	}
	result->unarange_num = count_num;

}

static INT16U get_addr_result(topo_data_t* data, topo_result_t* result)
{
	INT16U couple_num = 0;
	INT16U i;

	memset(&(result->result_meas_no[0][0]), 0, TOPO_METER_NUM * 2);

	for (i = 0; i < data->meter_num_ori; i++)
	{
		if (result->topo_result[i][0] + result->topo_result[i][1] != 0)
		{
			result->result_meas_no[couple_num][0] = data->measure_point_no[result->topo_result[i][0]];
			result->result_meas_no[couple_num][1] = data->measure_point_no[result->topo_result[i][1]];
			couple_num++;
		}
		if (couple_num == 105)
		{
			int stop = 1;
		}
	}

	for (i = 0; i < result->unarange_num; i++)
	{
		result->unarange_meas_no[i] = data->measure_point_no[result->unarange[i]];
	}

	return couple_num;
}

static void update_result_layer(topo_data_t* data, topo_result_t* result)
{
	INT8U layer_num = 0;
	INT8U layer_num_sure_flag = 0;
	INT16U child_id = 0;
	INT16U parent_id = 0;
	INT16U max_parent_id = 0;
	INT16U i, j;

	for (i = 0; i < data->meter_num_ori; i++)
	{
		child_id = result->topo_result[i][0];
		parent_id = result->topo_result[i][1];
		

		/* 若父节点与子节点id均为0 */
		if (child_id == 0  && parent_id == 0)
		{
			layer_num_sure_flag = 0;	// 层数不确定
			result->topo_result[i][3] = layer_num_sure_flag;
			continue;					// 有可能为最高父节点或该节点未给出拓扑信息两种情况
		}
		/* 遍历父子对，找出最长的拓扑层级 */
		else
		{
			layer_num += 1;
			layer_num_sure_flag = 1;
			result->topo_result[i][2] = layer_num;
			result->topo_result[i][3] = layer_num_sure_flag;
			for (j = 0; j < data->meter_num_ori; j++)
			{
				child_id = result->topo_result[j][0];
				/* 若父节点与子节点id均为0 */
				if (child_id == 0 && parent_id == 0)
				{
					continue;		
				}

				if (child_id == parent_id)			// 在子节点列中，找到上一层级的父节点，层数+1，更新父节点id
				{
					layer_num += 1;
					layer_num_sure_flag = 1;
					result->topo_result[i][2] = layer_num;
					result->topo_result[i][3] = layer_num_sure_flag;
					parent_id = result->topo_result[j][1];
				}
				else							   // 在子节点列中，找不到上一层的父节点，该节点为最高父节点
				{
					max_parent_id = result->topo_result[j][2];
				}	
			}
			layer_num = 0;				   // 层数清0
		}		
	}

	/* 考虑父节点id为0的情况 */
	for (i = 0; i < data->meter_num_ori; i++)
	{
		child_id = result->topo_result[i][0];
		if (child_id == max_parent_id)
		{
			max_parent_id = result->topo_result[i][1];
		}
	}

	for (i = 0; i < data->meter_num_ori; i++)
	{
		result->topo_result[i][4] = max_parent_id;
	}
}

INT16U BigDataTopoCalculate(topo_data_t* data, topo_voltage_t* voltage, topo_result_t* result, topo_cal_para_t* cal_para)
{


	int ret = 0;
	INT16U couple_num = 0;
#ifndef Used_in_Terminal
	ret = sqlite3_open("test0415.db", &db);  // sqlite3_open：SQLite库的一个函数，用于打开一个数据库连接。
#endif
	Init_Sql_Tabel();
	//STEP 1 : 数据预处理
	data_preprocessing(data, cal_para);
	if (data->used_sample_num < TOPO_SAMPLE_MIN)
	{
		return 0;
	}
	//STEP 2 : 电压聚类
	voltage_cluster(data, voltage, cal_para);
	//将数据写入数据表
	Write_To_Data_Table(data, voltage);
	//STEP 3 : 拓扑计算梳理
	topo_calculate(data, result, cal_para);
	//STEP 4 : 电压补挂
	voltage_complement(data, voltage, result);
	//STEP 5 : 写新的测量点父子对
	couple_num = get_addr_result(data, result);
	// STEP 6 : 增加拓扑层级结构
	update_result_layer(data, result);
	//写入结果表
	Write_To_Final_Result_Table(result, couple_num);

	return 1;
}


