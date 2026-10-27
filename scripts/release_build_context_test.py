"""Exercise the real Docker COPY boundary with public sentinel fixtures only."""

import pathlib
import subprocess
import tempfile
import unittest


class BuildContextTest(unittest.TestCase):
    def test_private_shell_inputs_and_build_outputs_stay_out(self):
        root = pathlib.Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(prefix="stellwerk-context-test-") as directory:
            work = pathlib.Path(directory)
            context = work / "context"
            context.mkdir()
            (context / ".dockerignore").write_bytes((root / ".dockerignore").read_bytes())
            public = {"go.mod": "module example.invalid/probe\n", "cmd/main.go": "package main\n"}
            private = [".env", ".env.local", ".envrc", ".envrc.local", ".direnv/state", ".git/config", "cover.out"]
            for name, contents in public.items():
                path = context / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(contents)
            for name in private:
                path = context / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("exclude this public test sentinel\n")
            dockerfile = work / "Dockerfile"
            dockerfile.write_text("FROM scratch\nCOPY . /\n")
            output = work / "output"
            result = subprocess.run(
                ["docker", "build", "--network=none", "--file", str(dockerfile),
                 "--output", f"type=local,dest={output}", str(context)],
                capture_output=True, text=True, timeout=90, check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            for name, contents in public.items():
                self.assertEqual((output / name).read_text(), contents)
            for name in private:
                self.assertFalse((output / name).exists(), name)


if __name__ == "__main__":
    unittest.main()
