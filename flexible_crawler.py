import requests
import re
import duckdb
import time
import argparse
import yaml
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# 创建线程锁，确保数据库操作线程安全
db_lock = Lock()

# 定义从YAML文件加载配置的函数
def load_config(config_path):
    """
    从YAML文件加载配置
    
    参数:
        config_path: YAML配置文件的路径
    
    返回:
        包含配置项的字典
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # 验证必要的配置项
        required_keys = ['dbPath', 'baseUrl', 'startPage', 'endPage']
        for key in required_keys:
            if key not in config:
                raise ValueError(f"配置文件缺少必要的键: {key}")
        
        # 设置默认值
        config.setdefault('firstPageUrl', '')
        config.setdefault('maxWorkers', 10)
        
        return config
    except FileNotFoundError:
        print(f"配置文件未找到: {config_path}")
        raise
    except yaml.YAMLError as e:
        print(f"解析配置文件出错: {e}")
        raise
    except Exception as e:
        print(f"加载配置出错: {e}")
        raise

# 定义爬取单页的函数
def crawl_page(page, base_url_pattern, first_page_url):
    # 构造列表页URL
    if page == 1 and first_page_url:
        list_url = first_page_url
    else:
        list_url = base_url_pattern.format(page=page)
    
    print(f'正在爬取: {list_url}')
    
    try:
        # 添加请求头，模拟浏览器行为
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2'
        }
        
        # 发送请求获取页面内容
        response = requests.get(list_url, headers=headers, timeout=15)
        response.raise_for_status()  # 检查请求是否成功
        
        # 确保内容编码正确
        response.encoding = 'utf-8'  # 显式设置编码为UTF-8
        
        # 使用BeautifulSoup解析HTML
        soup = BeautifulSoup(response.text, 'lxml')
        
        # 找到目标div：col-sm-9 col-xs-12 xs-padding
        target_div = soup.find('div', class_='col-sm-9 col-xs-12 xs-padding')
        
        if target_div:
            # 在target_div内查找所有包含文章信息的a标签
            article_items = []
            all_links = target_div.find_all('a', href=True)
            
            for link in all_links:
                href = link['href']
                # 匹配新闻详情页URL格式
                if re.match(r'^https://news\.ruc\.edu\.cn/\d+\.html$', href):
                    # 提取文章标题
                    title = ''
                    h5 = link.find('h5', class_='xs-padding xs-font-size')
                    if h5:
                        title = h5.text.strip()
                    
                    # 提取发布时间
                    publish_time = ''
                    # 查找可见的时间标签
                    visible_time = link.find('p', class_='visible-xs')
                    if visible_time:
                        publish_time = visible_time.text.strip()
                    # 如果没找到，查找隐藏的时间标签
                    if not publish_time:
                        time_div = link.find('div', class_='pp-box-time')
                        if time_div:
                            # 提取年月和日
                            span = time_div.find('span')
                            p = time_div.find('p')
                            if span and p:
                                publish_time = f"{span.text}.{p.text}"
                    
                    article_items.append({
                        'url': href,
                        'title': title,
                        'publish_time': publish_time
                    })
            
            return list_url, article_items
        else:
            print(f'第 {page} 页未找到目标div')
            return list_url, []
            
    except requests.RequestException as e:
        print(f'请求失败 {list_url}: {e}')
        return list_url, []
    except Exception as e:
        print(f'处理页面失败 {list_url}: {e}')
        import traceback
        traceback.print_exc()
        return list_url, []

# 主函数
def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='灵活的网页爬虫，可配置列表页URL模式')
    parser.add_argument('--config', '-c', default='./config.yaml', 
                       help='配置文件路径（YAML格式）')
    args = parser.parse_args()
    
    # 加载配置
    config = load_config(args.config)
    
    # 从配置中获取参数
    db_path = config['dbPath']
    start_page = config['startPage']
    end_page = config['endPage']
    max_workers = config['maxWorkers']
    base_url = config['baseUrl']
    first_page = config['firstPageUrl']
    
    # 创建或连接到DuckDB数据库
    conn = duckdb.connect(db_path)
    
    # 创建序列用于自动生成ID
    conn.execute('CREATE SEQUENCE IF NOT EXISTS article_urls_id_seq')
    
    # 创建表（如果不存在）
    conn.execute('''
        CREATE TABLE IF NOT EXISTS article_urls (
            id INTEGER PRIMARY KEY DEFAULT nextval('article_urls_id_seq'),
            url VARCHAR UNIQUE NOT NULL,
            title VARCHAR,
            publish_time VARCHAR,
            list_url VARCHAR NOT NULL,
            crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.close()
    
    total_pages = end_page - start_page + 1
    
    print(f'开始爬取 {start_page} 到 {end_page} 页，共 {total_pages} 页')
    print(f'列表页URL模式: {base_url}')
    if first_page:
        print(f'第一页特殊URL: {first_page}')
    print(f'使用 {max_workers} 个线程')
    print(f'数据库路径: {db_path}')
    
    start_time = time.time()
    total_urls = 0
    
    # 使用线程池执行爬取任务
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_page = {}
        for page in range(start_page, end_page + 1):
            future = executor.submit(crawl_page, page, base_url, first_page)
            future_to_page[future] = page
        
        # 处理完成的任务
        for future in as_completed(future_to_page):
            page = future_to_page[future]
            try:
                list_url, article_urls = future.result()
                
                # 将提取的URL、标题和发布时间存储到数据库（线程安全）
                if article_urls:
                    with db_lock:
                        conn = duckdb.connect(db_path)
                        try:
                            for item in article_urls:
                                # 确保item是字典类型
                                if isinstance(item, dict):
                                    conn.execute(
                                        'INSERT OR IGNORE INTO article_urls (url, title, publish_time, list_url) VALUES (?, ?, ?, ?)', 
                                        [item.get('url', ''), item.get('title', ''), item.get('publish_time', ''), list_url]
                                    )
                            conn.commit()
                        finally:
                            conn.close()
                
                total_urls += len(article_urls)
                print(f'第 {page} 页提取了 {len(article_urls)} 个URL')
            except Exception as e:
                print(f'第 {page} 页处理出错: {e}')
    
    end_time = time.time()
    
    # 最终统计
    print(f'\n爬取完成！')
    print(f'总耗时: {end_time - start_time:.2f} 秒')
    print(f'共爬取 {total_pages} 页')
    print(f'共提取 {total_urls} 个URL')
    
    # 验证数据库中的总数
    conn = duckdb.connect(db_path)
    total_in_db = conn.execute('SELECT COUNT(*) FROM article_urls').fetchone()[0]
    conn.close()
    
    print(f'数据库中存储了 {total_in_db} 个唯一URL')

if __name__ == '__main__':
    main()
