# Create spectrograms
import neuraltoolkit as ntk

rawdat_dir='/media/bs007r/CAF00052/CAF00052_2020-11-23_13-30-40/'
# Standard /media/HlabShare/Sleep_Scoring/ABC00001/LFP_chancheck/'
outdir='/media/HlabShare/Tovi_work/LFP_extraction/CAF00052/1123/hr2/'
hstype = ['hs64']

# hour: hour to generate spectrograms
# choose a representative hour with both NREM, REM and wake
hour = 2
# fs: sampling frequency (default 25000)
# nprobes : Number of probes (default 1)
# number_of_channels : total number of channels
# probenum : which probe to return (starts from zero)
# probechans : number of channels per probe (symmetric)
# lfp_lowpass : default 250

ntk.selectlfpchans(rawdat_dir, outdir, hstype, hour,
                   fs=25000, nprobes=1, number_of_channels=64,
                   probenum=0, probechans=64, lfp_lowpass=250)

# Now go through the spectrograms and select best lfp channels in
# the best probe to extract lfp
