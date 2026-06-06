"""
cli.py — Command-line entry point for the USMNT agent

Run with:
    python cli.py

Optional flags:
    python cli.py --debug     # show raw LLM outputs at each phase
"""

import sys
import argparse
import agent

BANNER = """
╔══════════════════════════════════════════════╗
║       USMNT Data Analyst Agent  v1.0        ║
║  Ask anything about the US Men's NT squad   ║
║  Type 'quit' or 'exit' to leave             ║
╚══════════════════════════════════════════════╝
"""

EXAMPLE_QUESTIONS = [
    "Which player has played the most minutes in the past year?",
    "Tell me about Christian Pulisic's most recent match.",
    "Who is the best performing away player on the squad?",
    "Are yellow cards correlated with goals scored?",
]


def main():
    parser = argparse.ArgumentParser(description="USMNT Data Analyst Agent")
    parser.add_argument("--debug", action="store_true", help="Show raw LLM outputs")
    args = parser.parse_args()

    if args.debug:
        agent.DEBUG = True

    print(BANNER)
    print("Example questions to get started:")
    for q in EXAMPLE_QUESTIONS:
        print(f"  • {q}")
    print()

    while True:
        try:
            question = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye.")
            sys.exit(0)

        if not question:
            continue

        if question.lower() in {"quit", "exit", "q"}:
            print("Goodbye.")
            sys.exit(0)

        print()
        answer = agent.run(question)
        print(f"\nAgent: {answer}\n")
        print("-" * 60)
        print()


if __name__ == "__main__":
    main()