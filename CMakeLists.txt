
# 指定CMake最低版本要求
cmake_minimum_required(VERSION 3.10)
# 创建工程
project(big_data_topo_project)

# add_compile_options(-Wall)
# 设置可执行文件路径
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
# 指定库的生成目录
set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)
# 添加头文件目录
include_directories(${PROJECT_SOURCE_DIR})
# 指定链接目录
link_directories(${LIBRARY_OUTPUT_PATH})

message(STATUS "gcc confirm: " ${CMAKE_C_COMPILER})

# 平台差别
string(FIND ${CMAKE_C_COMPILER} "mingw" x86_64_WINDOWS)

# 创建源代码文件列表
set(DEBUG_SRC_LIST
    big_data_topo.c
    sqlite3.c
    test.c 
)
set(LIB_SRC_LIST
    big_data_topo.c
    sqlite3.c
)
set(TESTLIB_SRC_LIST
    test.c 
)

# windows
if(${x86_64_WINDOWS} GREATER 0)
    # 打印cmake 配置信息
	message(STATUS "build for x86_windows")
    set(LINK_LIB_LIST
        topo
        pthread
        m)

else()
message(STATUS "build for linux")
    set(LINK_LIB_LIST
        topo
        pthread 
        dl
        m)
endif()


# 生成拓扑动态库
add_library(topo SHARED ${LIB_SRC_LIST})
# 生成调试用可执行文件
add_executable(topo_debug ${DEBUG_SRC_LIST})
# 生成测试动态库可执行文件
add_executable(topo_test_lib ${TESTLIB_SRC_LIST})
# 链接库
target_link_libraries(topo_test_lib ${LINK_LIB_LIST})
target_link_libraries(topo_debug dl m pthread)