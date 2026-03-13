# Real-time Data Lake and Pipeline Construction using AWS Cloud

<br>

## 🔍 서비스 소개

**실시간 개인화 마케팅 및 추천, 실시간 트렌드 분석, 실시간 보안 위협 감지 및 대응**과 같은 다양한 활용 사례에서 볼 수 있듯이, 데이터의 즉각적인 처리 및 활용 능력은 기업의 비지니스적 가치를 올리는데 매우 중요합니다. 이러한 시대적 요구에 발맞춰, **AWS 클라우드 서비스**를 기반으로 **Kafka와 Spark Streaming**을 활용하여 **종단 간(End-to-End) 실시간 데이터 레이크 및 파이프라인을 구축**합니다.

- [서울시 따릉이 데이터](https://data.seoul.go.kr/dataList/OA-15493/A/1/datasetView.do)를 실시간으로 수집하고 처리합니다.
- 서울시 공공데이터 포털에서 제공하는 공공자전거 실시간 대여 정보는, 특정 시점의 대여소별 실시간 자전거 잔여 대수, 즉 상태 데이터만을 제공합니다.
- 단순한 **상태 데이터(잔여 대수)를 실시간으로 수집하고 비교/가공함으로써, 데이터(대여/반납)로 재창조**합니다.

<br>

## ⚡️ 주요 기능

- **실시간 데이터 처리**
    - 15초 간격마다 API 호출, 잔여 대수가 감소되면 대여 /증가하면 반납으로 카운트
- **평균 기반 수요 예측**
    - 축적된 데이터를 기반으로 요일과 시간대별 자전거 잔여 거치량의 통계적 평균을 계산합니다.
    - 이를 통해 미래 특정 시각, 특정 지역의 자전거 예상 보유량을 사용자에게 제공합니다.

<br>

## ⚙️ 서비스 아키텍처 (Data Lake 아키텍처)

<img width="1871" height="1298" alt="Image" src="https://github.com/user-attachments/assets/bc427300-77f7-4f8f-9bb6-bdb8a342038b" />

1. 서울시 공공데이터에서 API를 호출
2. Kafka로 해당 데이터를 전송하는 Producer 구성
3. Kafka에 있는 데이터를 가져와 처리하기 위한 Spark Streaming 잡을 정의
4. 계산된 대여/반납 건수를 S3에 저장
5. AWS의 Glue, Athena라는 서비스를 통해 S3의 데이터를 테이블 형식으로 조회
6. Python Dash Board는 Athena 기능을 통해 S3에 저장되어있는 데이터를 시각화

<br>

## 🛠️ 주요 기술 스택

- **Confluent Python Kafka 클라이언트**를 통해 데이터를 수집합니다. Kafka의 Producer/Consumer 아키텍처를 통해 대량의 실시간 데이터를 안정적이고 효율적으로 수집 및 분배합니다.
- **Spark Streaming**을 활용한 실시간 데이터 처리 및 S3 저장
- **Athena**를 활용한 데이터 접근 및 질의
- **Hadoop**의 활용: hadoop을 주요 저장소로 사용하지는 않지만, Spark와 매우 밀접한 연관을 가지고 있기에 **HDFS(Hadoop Distributed FIle Sysyem)와 YARN(Yet Another Resource)**와 같은 Hadoop 에코 시스템 구성 요소를 Spark 클러스터 환경 설정 및 관리에 일부 사용합니다. **Spark는 Hadoop YARN 클러스터 관리자 위에서 동작**하도록 합니다.
(이를 통해자원 관리의 효율성과 안정성을 확보합니다.)
- 서버에 코드를 배포하는 과정을 자동화하기 위해 **GitHub Actions와 AWS Code Deploy를 활용하여 CI/CD 파이프라인**을 구축합니다.
- Kafka와 YARN은 클러스터를 구성하는데 있어, **Apache ZooKepper**가 필수적이므로, ZooKeeper의 분산 코디네이션을 클러스터 환경에 적용합니다.
- **모니터링**: 실시간 시스템 가시성 확보
- 위의 **복잡한 클라우드 인프라 구축 및 서비스 연동에 필요한 개발 공수를 줄이기 위해 Ansible 자동화 도구**를 활용합니다.

<br>

## 📡 인프라 구성

<img width="1007" height="341" alt="Image" src="https://github.com/user-attachments/assets/62b83c25-9bae-4be9-8817-90360cd43a38" />

- Kafka Cluster와 Spark Cluster(on YARN 환경 구축)

<br>

<img width="627" height="347" alt="Image" src="https://github.com/user-attachments/assets/1d0cacab-219e-404e-9c41-2e3cd2882ae2" />


- 3대의 Kafka 서버(kafka-broker01~03)와 3대의 Spark 서버(spark01~03)를 Private Subnet에 배치
- 각 서버를 서로 다른 가용 영역(Multi-AZ) ap-northeast-2b, 2c, 2d에 분산 배치하여, 특정 데이터 센터에 장애가 발생해도 서비스가 중단되지 않도록 고가용성을 구성
- NAT Gateway 대신 비용 효율적인 NAT 인스턴스용 EC2를 Public Subnet에 배치

<br>

<img width="816" height="346" alt="Image" src="https://github.com/user-attachments/assets/eddb89e0-0135-4dfc-a485-643e256ee5fd" />


- Github Actions 와 AWS Codedeploy를 사용하여 EC2까지 코드 배포
- 클러스터를 구성 및 설정시 Ansible을 사용

<br>

## 🔥 Trouble Shootings

- Kafka Producer의 여러 옵션 및 파라미터 설정을 통해, 멱등성 보장, 메시지 순서 보장, 메시지 압축을 통한 Producer/Consumer 초당 전송/수신률 상승
- Spark 프로그램에서의 Wide Transform 대응
- 정확한 대여/반납 건수 집계를 위한 이전 Micro Batch 상태 관리
- Spark Streaming의 Micro Batch 크기 불규칙성 및 Spark Job 재시작 시 데이터 처리 연속성 확보
- 초기 실행 및 데이터 단절 시나리오 시 집계 왜곡 방지
- Query Checkpointing 및 DataFrame Checkpointing 전략 사용을 통한, Source & State Fault-Tolerance
- Hive Metastore(MySQL 활용)을 통한 테이블 메타데이터 관리의 한계(Derby) 및 동시성 부재 해결
- S3 저장소의 효율적인 파일 관리

<br>

## 관련 리포지토리

- [Pyspark Apps](https://github.com/Realtime-DataLake/pyspark-apps)
- [Kafka Producer](https://github.com/Realtime-DataLake/kafka-producer)
- [Ansible Playbooks](https://github.com/Realtime-DataLake/datalake-ansible-playbook)
