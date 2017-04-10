package Server;

import java.io.*;
import java.net.Socket;

/**
 * Created by MingDong on 2016/9/8.
 */
public class Client {
    public static void main(String[] args) {
        Socket socket = null;
        BufferedReader br = null;
        PrintWriter pw = null;
        try {
            //客户端socket指定服务器的地址和端口号
            socket = new Socket("192.168.1.109", 9999);
            System.out.println("Socket=" + socket);
            //同服务器原理一样
            br = new BufferedReader(new InputStreamReader(
                    socket.getInputStream()));
            pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(
                    socket.getOutputStream())));
            //String str = "中国 卫生部 官员 24日 2005 年底 中国 报告 尘肺病 病人 累计 已超过 60万例 职业病 整体 防治 形势严峻 卫生部 副部长 当日 国家 职业 卫生 示范 企业 授牌 企业 职业 卫生 交流 大会 上说 中国 各类 急性 职业 中毒 事故 发生 200 多起 上千人 中毒 直接经济损失 上百 亿元 职业病 病人 量大 发病率 较高 经济损失 影响 恶劣 卫生部 24日 公布 2005年 卫生部 收到 全国 30个 自治区 直辖市 不包括 西藏 各类 职业病 报告 12212例 尘肺病 病例 报告 9173例 75.11 矽肺 煤工尘肺 中国 最主要 尘肺病 尘肺病 发病 工龄 缩短 去年 报告 尘肺病 病人 最短 时间 三个月 平均 发病 年龄 40.9岁 最小 发病 年龄 20岁 政府部门 执法不严 监督 企业 生产水平 不高 技术 设备 落后 职业 卫生 原因 原因是 企业 法制观念 淡薄 社会责任 缺位 缺乏 维护 职工 健康 意识 职工 合法权益 保障 提高 企业 职业 卫生 工作 重视 卫生部 国家安全 生产 监督管理 总局 中华全国总工会 24日 在京 选出 56家 国家级 职业 卫生 工作 示范 企业 希望 企业 社会 推广 职业 病防治 经验 促使 企业 作好 职业 卫生 工作 保护 劳动者 健康 ";
           String str = "当天 沈阳 晚报 沈阳网 记者 就 这个 问题 咨询 中国银行 沈阳 奥体中心 支行 银行 工作 人员 表示 虽然 目前 市面上 已经 很少 见" +
                   "但 第四套 人民币 并未 停止 流通" +
                   "银行 为 ATM机 配款 时 不会 投放 第四套 人民币" +
                   "但 不排除 其他 储户 将 旧版 人民币 存入 ATM机，" +
                   "而 ATM机 属于 存取款 一体机 存款箱 和 取款箱 是 循环 的。";
            pw.println(str);
            pw.flush();

            String result = br.readLine();
            System.out.println(result);

            pw.println("END");
            pw.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                System.out.println("close......");
                br.close();
                pw.close();
                socket.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
