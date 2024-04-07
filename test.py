'''
Description: 
Date: 2023-12-22 15:38:22
LastEditors: yupengyang yupengyang@topscomm.com
LastEditTime: 2024-04-07 14:52:12
Author: 
'''
import gradio as gr
import requests

# def ans(chatbot,query,chioce):
#     # chatbot ：历史聊天框
#     # query:用户的当前问题
#     # choice：
#     url = "http://i-2.gpushare.com:45911/answer"
#
#     param = {
#         "sentence": query
#     }
#
#     res = requests.post(url, json=param)
#
#     res = res.json()
#
#     ans = res["ans"]
#
#     return chatbot + [[query,ans]]
def ans(chatbot,query,chioce):
    ans = [query, '欢迎来到布丁的机器人!']
    chatbot.append(ans)
    return chatbot


with gr.Blocks(title="布丁的机器人") as graio_demo:  # 访问127.0.0.1:10998

    gr.HTML(""" <h1 align="center"> buding的机器人  </h1> """)  # 设置一级标题，居中表达
    chioce = gr.Radio(choices = ["本地模型","知乎回答","百度知道","CSDN"],label="功能选择")
    chatbot = gr.Chatbot([[None,"欢迎来到布丁的机器人!"]],show_label=False)  # 定义聊天框，None：用户提问。。show_label：不显示chatbox
    query = gr.Textbox(show_label=False,placeholder="请输入:") # 文本框，提问框

    with gr.Row():  # 创建行
        with gr.Column():  # 创建列
            button1 = gr.Button(value="生成回答")  # 回车按钮
        with gr.Column():
            button2 = gr.Button(value="清空历史")  # 回车按钮
    button1.click(ans,inputs=[chatbot,query,chioce],outputs=[chatbot])  # 创建回车函数，返回问答对 chatbot：历史聊天记录，query：用户输入，choice：模型类别
    query.submit(ans,inputs=[chatbot,query,chioce],outputs=[chatbot])  # 绑定回车，回车等于执行

graio_demo.launch(share=False)  # 启动，服务器的地址，端口号