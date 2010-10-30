import hotshot, hotshot.stats

stats = hotshot.stats.load("hotshot.prof")
stats.strip_dirs()
# stats.sort_stats('cumulative', 'calls')
stats.sort_stats('time', 'calls')
stats.print_stats(20)