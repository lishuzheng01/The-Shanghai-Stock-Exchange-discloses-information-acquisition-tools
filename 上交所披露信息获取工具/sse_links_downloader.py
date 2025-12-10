import csv
import time
import requests

# ================== 配置区 =====================

# 要处理的股票代码列表（按需修改）
CODES = ["600000", "600519", "601318"]

# 年份范围（闭区间）
START_YEAR = 2022
END_YEAR = 2023

# 上交所定期报告查询接口
URL_QUERY_COMPANY = "https://query.sse.com.cn/security/stock/queryCompanyBulletin.do"
# PDF 基础地址
URL_PDF_BASE = "https://static.sse.com.cn"

HEADERS = {
    "Referer": "https://www.sse.com.cn/disclosure/listedinfo/announcement/",
    "User-Agent": "Mozilla/5.0",
}

MAX_RETRIES = 3


# ================== 工具函数 =====================

def request_with_retry(session, method, url, max_retries=MAX_RETRIES, **kwargs):
    """带重试的请求封装（不下载文件，只拉 JSON）"""
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.request(method, url, timeout=15, **kwargs)
            resp.raise_for_status()
            return resp
        except Exception as e:
            print(f"[WARN] 请求失败 {attempt}/{max_retries} 次: {e}")
            if attempt == max_retries:
                raise
            time.sleep(2 * attempt)


def fetch_reports_for_year(session, code, year):
    """获取某股票某年的定期报告列表（年报）"""
    begin_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    params = {
        "isPagination": "false",       # 不分页，一次性返回
        "productId": code,             # 股票代码
        "keyWord": "",
        "securityType": "0101",        # 主板
        "reportType2": "DQBG",         # 定期报告
        "reportType": "YEARLY",        # YEARLY=年报
        "beginDate": begin_date,
        "endDate": end_date,
    }

    resp = request_with_retry(session, "GET", URL_QUERY_COMPANY,
                              headers=HEADERS, params=params)
    data = resp.json()
    results = data.get("result", [])
    print(f"[INFO] {code} {year} 年共获取到 {len(results)} 条记录")
    return results


# ================== 主逻辑：只汇总链接 =====================

def main():
    session = requests.Session()

    for year in range(START_YEAR, END_YEAR + 1):
        print(f"\n===== 处理年份：{year} =====")
        summary_rows = []      # 该年份所有记录
        seen_urls = set()      # 去重用：URL 集合

        for code in CODES:
            print(f"\n[CODE] 处理股票：{code}")
            try:
                reports = fetch_reports_for_year(session, code, year)
            except Exception as e:
                print(f"[ERROR] 获取 {code} {year} 年报告失败：{e}")
                continue

            for item in reports:
                title = item.get("TITLE", "").strip()
                date = item.get("SSEDATE", "").strip()
                relative_url = item.get("URL", "").strip()

                if not relative_url:
                    continue

                # 补全为完整链接
                if not relative_url.startswith("http"):
                    pdf_url = URL_PDF_BASE + relative_url
                else:
                    pdf_url = relative_url

                # 按 URL 去重
                if pdf_url in seen_urls:
                    print(f"[DUP] 已存在，跳过：{pdf_url}")
                    continue
                seen_urls.add(pdf_url)

                # 只记录信息，不下载
                print(f"[LINK] {code} | {date} | {title} | {pdf_url}")

                summary_rows.append({
                    "code": code,
                    "title": title,
                    "date": date,
                    "url": pdf_url,
                })

                time.sleep(0.2)

        # 写 CSV：标题，日期，链接（和代码）
        if summary_rows:
            csv_name = f"summary_links_{year}.csv"
            with open(csv_name, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(
                    f, fieldnames=["code", "title", "date", "url"]
                )
                writer.writeheader()
                writer.writerows(summary_rows)
            print(f"\n[OK] {year} 年链接汇总已写入：{csv_name}")
        else:
            print(f"\n[WARN] {year} 年没有任何记录。")

    print("\n全部任务完成！")


if __name__ == "__main__":
    main()
