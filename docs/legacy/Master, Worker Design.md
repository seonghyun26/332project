# Master, Worker Design

![Untitled](Master,%20Worker%20Design/Untitled.png)

# Worker Concept

- Worker는 여러 블럭(파일)을 가지고 블럭들을 리스트로 관리한다.
- Worker는 다음과 같은 함수들을 가진다.

![Untitled](Master,%20Worker%20Design/Untitled%201.png)

### Block

- Block은 다음의 attribute를 가진다.

![Untitled](Master,%20Worker%20Design/Untitled%202.png)

### Tuple

- Tuple은 다음의 attribute를 가진다.

![Untitled](Master,%20Worker%20Design/Untitled%203.png)

## Design Consideration Point

- Temporary file의 이름을 어떻게 정할 것인가?
    - 자동으로 지정되도록
    - 마스터가 명시적으로 설정하도록
- Block의 튜플을 읽어오는 스트림을 반환하는 toStream은 얼마나 lazy하게 tuple을 load할 것인가?
    - 파일에서 tuple을 미리 읽어온 다음 버퍼를 통해 stream 구현
    - stream의 매 다음 원소 evaluate마다 파일을 읽기

# Master Concept

- Master는 다음의 세가지 작업을 한다.
    - Worker에게서 sample을 받아 partition을 나눈다.
    - 모든 worker가 phase에 진입할 준비를 마쳤을 때, 각 worker의 작업을 시작하도록 한다.
    - Block transfer phase에서 worker가 어떤 worker로 파일을 전송할지를 결정한다.

# Network Connection

- Worker는 각 작업을 끝냈을 때, Master에게 현재 작업이 끝났음을 알리는 request를 보내고, response가 되돌아올 때 까지 기다린다.
- Master는 worker들이 작업을 synchronize해야할 필요가 있을 경우 response를 보내는 것을 미룬다. 모든 worker가 phase에 진입할 준비를 마쳤을 때 response를 보내도록 한다.

⇒ Master의 request handler는 concurrent하게 작동하므로, concurrency의 issue를 고려해야 한다.

# Phase

Worker는 다음의 5개 phase로 구성되어 있다.

1. Initialization
2. Sample
3. Sort & Partitioning
4. Block Transfer
5. Merge