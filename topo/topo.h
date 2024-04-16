/*
* File: Big data topo.h
* Author: Intelligent algorithms group
* Created on 2023.11
*/

#pragma once
#ifndef __TOPOCALCULATE_H_  
#define __TOPOCALCULATE_H_

//#define Used_in_Terminal        // ���뿪�أ������������������ֲ�նˣ��رգ���λ������
#include <math.h>
#include "sqlite3.h"              // ����SQLite���ݿ�
#pragma comment(lib,"sqlite3")    // ��SQLite���ݿ����ӵ��ó�����

// �ض�����������
typedef unsigned    char     INT8U;        // �����޷�������
typedef signed      char     INT8S;        // �����з�������
typedef unsigned    short    INT16U;       // �����޷��Ŷ�����
typedef signed      short    INT16S;       // �����з��Ŷ�����
typedef unsigned    int      INT32U;       // �����޷�������
typedef signed      int      INT32S;       // �����з�������
typedef             float    FP32;         // ���嵥���ȸ�����
typedef             double   FP64;         // ����˫���ȸ�����

#ifndef TRUE
#define TRUE 0x01
#endif

#ifndef FALSE
#define FALSE 0x00
#endif

#ifndef NULL
#define NULL   0UL
#endif

/* �����������Ŀά�� */
#define TOPO_SAMPLE_NUM      300
#define TOPO_VOLTAGE_NUM     100
/* ���������Ŀά�� */
#define TOPO_METER_NUM       1000     // ע��������������һ�г����ʵ����ദ��(1000 - 1�����
#define TOPO_THRIPLE_NUM     300 
/* һ��ʶ���и��ڵ������Ŀά�� */
#define TOPO_PARENTMAXNUM    200
/* �㷨ִ�е���С��Ч������ */
#define TOPO_SAMPLE_MIN      40
/* ��ַ���� */
#define TOPO_MAX_ADDR_LEN    20

/* �豸��־λ */
#define TOPO_METER_SINGLE    0               // �������־λ0
#define TOPO_METER_THREE     1               // �������־λ1
#define TOPO_NORMAL_LTU      2               // LTU��־λ2          
#define TOPO_FIRST_BRANCH    3               // һ�����ֿ���3
#define TOPO_MASTER_METER    98              // �����ܱ�98

/* �㷨���� */
#define TOPO_L2             1.0f             // L2���򻯲���
#define TOPO_RELA_COEF      0.0f             // ����Գͷ�����
#define TOPO_L1_PARA_DOWN   0.20f            // L1�ͷ������

/* ���˹����б���ֵ */
#define TOPO_JUDGE_LIMIT            0.5f             // ���˹����б���ֵ
#define TOPO_MAX_BETA_LIMIT         0.1f             // �������beta��ֵ
#define TOPO_FIT_JUDGE_MAE          0.2f             // ��������ж�ָ��MAE��ֵ
#define TOPO_VOLTAGE_CLUSTER_LIMIT  0.98f            // ���˵�ѹ������ֵ
#define TOPO_PEARSON_LIMIT          0.8f             // ����©�ұ����������ֵ

/* ����ṹ��data_t, �����洢�ӱ����ܱ������ݼ��ӱ�������ʵ��ʹ�õ������������� */
typedef struct
{
	FP64 child_meter_ori[TOPO_SAMPLE_NUM][TOPO_METER_NUM];      // ԭʼ�ӱ����ʶ���
	FP64 child_meter[TOPO_SAMPLE_NUM][TOPO_METER_NUM];          // �������ӱ����ʶ���
	INT16U CT[TOPO_METER_NUM];                                  // ���CT
	INT16U PT[TOPO_METER_NUM];                                  // ���PT
	INT16U device_type[TOPO_METER_NUM];                         // �豸���ͺ�
	INT16U ind_original2cluster[TOPO_METER_NUM];                // �������ԭʼ�豸��ŵĶ�Ӧ��ϵ������[1 2 2 2 3 4 5 5 6]
	INT16U used_sample_num;                                     // ʵ��������
	INT16U used_meter_num;                                      // ʵ�ʵ����(�����ᷢ���仯)
	INT16U used_voltage_num;                                    // ʵ�ʵ�ѹ����
	INT16U meter_num_ori;                                       // ����ǰ�豸��
	INT16U measure_point_no[TOPO_METER_NUM];                    // ������� 

}topo_data_t;

/* ����ṹ��data_t, ���ڴ洢���ࡢ�����ѹ��Ϣ������ */
typedef struct
{
	FP64 voltage_TRIPLE[TOPO_VOLTAGE_NUM][TOPO_THRIPLE_NUM][3];		           // �����ѹ����
	FP64 voltage_SINGLE[TOPO_VOLTAGE_NUM][TOPO_METER_NUM - TOPO_THRIPLE_NUM];  // �����ѹ����
	INT16U ind_TRIPLE[TOPO_THRIPLE_NUM];                                       // �����Ӧ��ԭʼ�豸���
	INT16U ind_SINGLE[TOPO_METER_NUM - TOPO_THRIPLE_NUM];                      // �����Ӧ��ԭʼ�豸���

}topo_voltage_t;

/* ����ṹ��data_t�����ڴ洢���������������� */
typedef struct
{
	FP64 beta[TOPO_METER_NUM];                          // ϵ��
	INT16U result_meas_no[TOPO_METER_NUM][3];           // ������ż�ĸ��ӶԹ�ϵ
	INT16U topo_result[TOPO_METER_NUM][5];              // �ӽڵ� -- ���ڵ� -- �ӽڵ������㼶 -- �Ƿ�ȷ�� -- ��߸��ڵ�
	INT16U* unarange;                                   // δ�Ϲҵı�����ţ����ã�ָ��cal_para_t���used_index
	INT16U unarange_meas_no[TOPO_METER_NUM];            // δ�ϹҵĲ������
	INT16U unarange_num;                                // δ�Ϲҵı���

}topo_result_t;

/* ����ṹ��data_t, ���ڴ洢�����ж���Ӻ����õ��ı��� */
typedef struct
{
	FP64 beta_steps[TOPO_METER_NUM];                    // ���������betaֵ
	FP64 parent_meter[TOPO_SAMPLE_NUM];
	FP64 fit[TOPO_SAMPLE_NUM];                          // ���ֵ
	FP64 weight[TOPO_SAMPLE_NUM];                       // Ȩ�أ�Huber���ͼ����Ȩ���ָ��ʱ�õ�
	FP64 XTX[TOPO_METER_NUM][TOPO_METER_NUM];           // Э�������
	FP64 U[TOPO_METER_NUM][TOPO_METER_NUM];             // ��������U����
	FP64 repenalty[TOPO_METER_NUM];                     // ȥ����Գͷ�����
	FP64 beta_last[TOPO_METER_NUM];                     // ��һ������betaֵ
	FP64 lambda[TOPO_METER_NUM];                        // L1 + L2 + ȥ����Գͷ�������
	FP64 residual[TOPO_SAMPLE_NUM];                     // ��ϲв�
	FP64 residual1[TOPO_SAMPLE_NUM];                    // ��ϲв�1
	FP64 max_beta[TOPO_METER_NUM];
	FP64 max_weight[TOPO_METER_NUM];
	INT16U used_index[TOPO_METER_NUM];                  // ����ÿ�����ڵ�������ı���
	INT16U used_index_steps[TOPO_METER_NUM];            // ���������в������ı���
	INT16U used_parent_meter_index[TOPO_PARENTMAXNUM];  // ���ڵ����
	INT16U used_num;                                    // ����ÿ�����ڵ�������ı���
	INT16U meter_num_ori;                               // ����ǰ���豸��

}topo_cal_para_t;

/* ����ṹ��matrix_para_t���������洢ϵ�������� */
typedef struct
{
	INT16U row1;             // ��
	INT16U column1;          // ��

}topo_matrix_para_t;

/* ����ṹ��huberfun_para_t�����ڴ洢�Ӻ���huberfunction�Ĳ��� */
typedef struct
{
	FP64* X;				// �ӱ�����
	FP64* Y;				// ĳ�����ڵ�
	FP64* meter_one;
	FP64* meter_two;
	FP64* square_x;
	FP64* lambda;				        // L1 + L2 + ȥ����Գͷ�������
	FP64* weight;				        // ����Ȩ��
	FP64* XTX;				            // ��������L����
	FP64* U;				            // ��������U����
	INT16U* used_id;			        // ����ÿ�����ڵ����ĵ��
	INT16U used_num;			        // ����ÿ�����ڵ����ĵ����
	INT16U used_meter_num_ori;		    // ����ǰ�豸��
	topo_matrix_para_t* matrixpara;		// ���ϵ�����������

}topo_huberfun_para_t;

/* �������� */
INT16U BigDataTopoCalculate(topo_data_t* data, topo_voltage_t* voltage, topo_result_t* result, topo_cal_para_t* cal_para);	// ���������װ��һ������

#endif






