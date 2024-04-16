#include <stdio.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>
#include "topo.h"
#include<pthread.h>


/* ��ʼ���ṹ�� */
topo_data_t data = { 0 };
topo_result_t result = { 0 };
topo_cal_para_t cal_para = { 0 };
topo_voltage_t voltage = { 0 };


const char* meter_name = "data_child_meter_ori.txt";
const char* type_name = "data_device_type.txt";
const char* voltage_single_name = "voltage_single.txt";
const char* voltage_LTUA_name = "LTU_voltage_A.txt";
const char* voltage_LTUB_name = "LTU_voltage_B.txt";
const char* voltage_LTUC_name = "LTU_voltage_C.txt";
const char* meas_point_name = "measure_point.txt";

int main()
{
	clock_t start_time = 0, end_time = 0;
	FILE* fp = NULL;
	INT16U i, j;

	INT16U used_samples_num = 80;
	INT16U used_meter_num = 206;
	INT16U single_meter_num = 201;
	INT16U triple_meter_num = 5;

	const char* path = "../data/";

	char addr_meter[200] = { 0 };
	char addr_type[200] = { 0 };
	char addr_voltage_single[200] = { 0 };
	char addr_voltage_A[200] = { 0 };
	char addr_voltage_B[200] = { 0 };
	char addr_voltage_C[200] = { 0 };
	char addr_meas_point[200] = { 0 };

	strcpy(addr_meter, path);
	strcat(addr_meter, meter_name);
	strcpy(addr_type, path);
	strcat(addr_type, type_name);
	strcpy(addr_voltage_single, path);
	strcat(addr_voltage_single, voltage_single_name);
	strcpy(addr_voltage_A, path);
	strcat(addr_voltage_A, voltage_LTUA_name);
	strcpy(addr_voltage_B, path);
	strcat(addr_voltage_B, voltage_LTUB_name);
	strcpy(addr_voltage_C, path);
	strcat(addr_voltage_C, voltage_LTUC_name);
	strcpy(addr_meas_point, path);
	strcat(addr_meas_point, meas_point_name);

	/* ������ */
	if ((fp = fopen(addr_meter, "r")) == NULL)
	{
		printf("failed\n");
		exit(0);
	}
	else
	{
		for (i = 0; i < used_samples_num; i++)
		{
			for (j = 0; j < used_meter_num; j++)
			{
				fscanf(fp, "%lf", &(data.child_meter_ori[i][j]));
				data.CT[j] = 1;
				data.PT[j] = 1;
			}
		}
		printf("Read meter file succeed! \n");
	}
	fclose(fp);		// ����ļ���ƥ��

	/* �ṹ��data������ֵ */
	data.meter_num_ori = used_meter_num;
	data.used_meter_num = used_meter_num;
	data.used_sample_num = used_samples_num;

	/* ���豸���ͺ� */
	if ((fp = fopen(addr_type, "r")) == NULL)
	{
		printf("failed\n");
		exit(0);
	}
	else
	{
		for (j = 0; j < used_meter_num; j++)
		{
			fscanf(fp, "%d", (int*)&(data.device_type[j]));
		}
		printf("Read device type file succeed!\n");
	}
	fclose(fp);

	/* ��������� */
	if ((fp = fopen(addr_meas_point, "r")) == NULL)
	{
		printf("failed!\n");
		exit(0);
	}
	else
	{
		for (j = 0; j < used_meter_num; j++)
		{
			fscanf(fp, "%d", (int*)&(data.measure_point_no[j]));
		}
		printf("Read measure point file succeed!\n");
	}
	fclose(fp);

	/* ��LTU��ѹA */
	if ((fp = fopen(addr_voltage_A, "r")) == NULL)
	{
		printf("failed!\n");
		exit(0);
	}
	else
	{
		for (i = 0; i < used_samples_num; i++)
		{
			for (j = 0; j < triple_meter_num; j++)
			{
				fscanf(fp, "%lf", &(voltage.voltage_TRIPLE[i][j][0]));
			}
		}
		printf("Read phase A file succeed!\n");
	}
	fclose(fp);

	/* ��LTU��ѹB */
	if ((fp = fopen(addr_voltage_B, "r")) == NULL)
	{
		printf("failed!\n");
		exit(0);
	}
	else
	{
		for (i = 0; i < used_samples_num; i++)
		{
			for (j = 0; j < triple_meter_num; j++)
			{
				fscanf(fp, "%lf", &(voltage.voltage_TRIPLE[i][j][1]));
			}
		}
		printf("Read phase B file succeed!\n");
	}
	fclose(fp);

	/* ��LTU��ѹC */
	if ((fp = fopen(addr_voltage_C, "r")) == NULL)
	{
		printf("failed!\n");
		exit(0);
	}
	else
	{
		for (i = 0; i < used_samples_num; i++)
		{
			for (j = 0; j < triple_meter_num; j++)
			{
				fscanf(fp, "%lf", &(voltage.voltage_TRIPLE[i][j][2]));
			}
		}
		printf("Read phase C file succeed!\n");
	}
	fclose(fp);

	/* ���������ѹ */
	if ((fp = fopen(addr_voltage_single, "r")) == NULL)
	{
		printf("failed!\n");
		exit(0);
	}
	else
	{
		for (i = 0; i < used_samples_num; i++)
		{
			for (j = 0; j < single_meter_num; j++)
			{
				fscanf(fp, "%lf", &(voltage.voltage_SINGLE[i][j]));
			}
		}
		printf("Read meter file succeed!\n");
	}
	fclose(fp);
	data.used_voltage_num = used_samples_num;

	start_time = clock();
	BigDataTopoCalculate(&data, &voltage, &result, &cal_para);
	end_time = clock();

	printf("\nchild_no\tparent_no\tchild_no_layer_num\tsure_flag\tmax_parent_id\n");
	for (i = 0; i < data.meter_num_ori; i++)
	{
		printf("%d\t\t%d\t\t\t%d\t\t%d\t\t%d\n", result.topo_result[i][0], result.topo_result[i][1], result.topo_result[i][2], result.topo_result[i][3], result.topo_result[i][4]);
	}

	FILE* fp1 = NULL;
	fp1 = fopen("result.txt", "w");
	fprintf(fp1, "\nchild_no\tparent_no\tchild_no_layer_num\tsure_flag\tmax_parent_id\n");
	for (i = 0; i < data.meter_num_ori; i++)
	{
		fprintf(fp1, "%d\t\t%d\t\t\t%d\t\t%d\t\t%d\n", result.topo_result[i][0], result.topo_result[i][1], result.topo_result[i][2], result.topo_result[i][3], result.topo_result[i][4]);
		
	}
	fclose(fp1);

	printf("\nTotal time consumption: %lf s", (FP32)(end_time - start_time) / CLOCKS_PER_SEC);
	getchar();

	return 0;
}
