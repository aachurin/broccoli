from . utils import get_colorizer, color


def splash(configs):

    index = 0
    indent = 0
    max_height = 16
    colorize = get_colorizer()

    def vshift():
        nonlocal index, indent
        index = 0
        if not lines:
            indent = 0
        else:
            indent = max(l[0] for l in lines) + 1

    def echo(text, endline=True, header=False, color=color.aqua):
        nonlocal index
        if index >= max_height:
            vshift()
        elif header and (index + 1) >= max_height:
            vshift()
        while index >= len(lines):
            lines.append([0, ''])
        line = lines[index]
        if line[0] < indent:
            line[1] += ' ' * (indent - line[0])
            line[0] = indent
        line[0] += len(text) + endline
        line[1] += colorize(text, color) + (' ' if endline else '')
        if endline:
            index += 1

    def echo_kv(key, value, max_width=35):
        key = '. ' + key + ': '
        echo(key, color=color.tea, endline=False)
        if isinstance(value, (list, tuple)):
            value = ', '.join(value)
        value = str(value)
        rest = max_width - len(key)
        indent = ''
        while value:
            echo(indent + value[:rest], color=color.olive)
            value = value[rest:]
            rest = max_width - 4
            indent = '    '

    def echo_conf(conf):
        if not conf:
            echo('. enabled', color=color.tea)
        for key, value in conf.items():
            echo_kv(key, value)

    lines = [
        [0, 0, 0, 0, 0, 0, 0, 233, 233, 58, 58, 149, 107, 149, 149, 106, 106,
         149, 149, 149, 107, 107, 64, 242, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        [0, 0, 0, 0, 234, 58, 106, 106, 106, 106, 106, 106, 106, 106, 106, 106,
         106, 106, 106, 106, 106, 106, 106, 106, 149, 64, 235, 0, 0, 0, 0,
         0, 0, 0],
        [0, 0, 235, 70, 106, 106, 106, 106, 106, 106, 106, 70, 106, 64, 106,
         64, 106, 70, 106, 64, 106, 106, 106, 106, 106, 106, 106, 106, 149,
         233, 0, 0, 0, 0],
        [0, 234, 70, 64, 106, 64, 64, 64, 64, 64, 64, 64, 70, 64, 64, 64, 64,
         64, 64, 64, 64, 106, 106, 106, 106, 70, 106, 106, 106, 235, 0, 0,
         0, 0],
        [0, 235, 70, 64, 64, 58, 22, 236, 235, 64, 64, 64, 64, 64, 64, 64, 64,
         64, 64, 64, 64, 64, 70, 70, 64, 70, 70, 70, 106, 106, 106, 232, 0,
         0],
        [0, 237, 64, 64, 64, 22, 237, 58, 234, 236, 106, 236, 22, 58, 236, 58,
         106, 236, 64, 64, 64, 64, 64, 64, 64, 64, 64, 70, 64, 106, 106, 58,
         0, 0],
        [0, 235, 64, 64, 64, 22, 22, 235, 149, 237, 106, 100, 238, 58, 149,
         100, 58, 22, 236, 236, 64, 64, 64, 64, 64, 64, 64, 70, 64, 64, 106,
         106, 234, 0],
        [0, 0, 0, 0, 236, 22, 22, 22, 3, 149, 235, 149, 149, 238, 237, 149,
         236, 149, 149, 236, 236, 3, 22, 237, 236, 236, 235, 235, 235, 22,
         106, 106, 64, 0],
        [0, 0, 0, 0, 0, 0, 0, 0, 237, 143, 149, 149, 58, 106, 149, 143, 106,
         58, 149, 149, 3, 58, 3, 106, 100, 235, 22, 22, 22, 22, 2, 70, 238,
         0],
        [0, 0, 0, 0, 0, 0, 0, 235, 149, 149, 149, 149, 106, 149, 149, 149, 149,
         106, 149, 149, 149, 149, 100, 100, 58, 235, 22, 22, 22, 22, 64,
         234, 0, 0],
        [0, 0, 0, 0, 0, 0, 235, 149, 149, 149, 149, 100, 149, 149, 149, 149,
         106, 149, 149, 58, 238, 22, 22, 22, 22, 22, 22, 22, 22, 22, 58,
         235, 0, 0],
        [0, 0, 0, 0, 233, 3, 149, 149, 149, 149, 149, 149, 149, 149, 149, 149,
         149, 237, 0, 0, 0, 0, 0, 0, 237, 234, 234, 234, 234, 234, 0, 0, 0,
         0],
        [0, 0, 0, 235, 58, 235, 149, 149, 149, 149, 149, 149, 149, 149, 149,
         236, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        [0, 0, 0, 237, 228, 228, 143, 143, 58, 58, 149, 149, 149, 100, 234, 0,
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        [0, 0, 0, 0, 0, 58, 228, 228, 228, 234, 236, 149, 235, 0, 0, 0,
         'b', 'r', 'o', 'c', 'c', 'o', 'l', 'i', ' ', 'v', '0', '.', '1',
         0, 0, 0, 0, 0],
        [0, 0, 0, 0, 0, 0, 0, 232, 237, 235, 58, 0, 0, 0, 0, 0, '(', 's', 't', 'a', 'r', 't',
         'e', 'r', ' ', 'k', 'i', 't', ')', 0, 0, 0, 0]
    ]

    if colorize.support_colors:
        lines = [[len(line), (''.join([colorize(
            ' ' if c == 0 else (c if type(c) is str else '#'),
            c if type(c) is int else color.grey
        ) for c in line]))] for line in lines]
    else:
        lines = []

    print()
    vshift()

    for header, config in configs:
        echo('[%s]' % header, header=True, color=color.white)
        echo_conf(config)
        echo('')

    print('\n'.join(l[1] for l in lines))
    print()
